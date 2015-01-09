/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.kafka

import java.awt.RenderingHints.Key
import java.io.Serializable
import java.nio.charset.StandardCharsets
import java.util.Properties
import java.{util => ju}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.eventbus.EventBus
import com.typesafe.scalalogging.slf4j.Logging
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.message.MessageAndMetadata
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.DefaultDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.commons.lang3.RandomStringUtils
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.feature.AvroFeatureDecoder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

object KafkaDataStore {
  def writeSchema(featureType: SimpleFeatureType,
                  topic: String,
                  producer: Producer[Array[Byte], Array[Byte]]): Unit = {
    val encodedSchema = SimpleFeatureTypes.encodeType(featureType).getBytes(StandardCharsets.UTF_8)
    val schemaMsg = new KeyedMessage[Array[Byte], Array[Byte]](topic, "schema".getBytes(), encodedSchema)
    producer.send(schemaMsg)
  }
}
class KafkaDataStore(broker: String, zookeepers: String, zkPath: String, isProducer: Boolean)
  extends ContentDataStore
  with Logging {
  import scala.collection.JavaConverters._

  private val groupId = RandomStringUtils.randomAlphanumeric(5)
  val zkClient = new ZkClient(zookeepers)

  override def createTypeNames() =
    zkClient.getChildren(zkPath).asScala.map(new NameImpl(_)).toList.asJava.asInstanceOf[java.util.List[Name]]

  override def createSchema(featureType: SimpleFeatureType) = {
    val typeName = featureType.getTypeName
    val path = getZkPath(typeName)
    if (zkClient.exists(path)) {
      throw new IllegalArgumentException(s"Type $typeName already exists")
    } else {
      // create the parent path if required
      if (!zkClient.exists(zkPath)) {
        try {
          zkClient.createPersistent(zkPath, true)
        } catch {
          case e: ZkNodeExistsException => // it's ok, something else created before we could
          case e: Exception => throw new RuntimeException(s"Could not create path in zookeeper at $zkPath", e)
        }
      }
      val data = SimpleFeatureTypes.encodeType(featureType)
      try {
        zkClient.createPersistent(path, data)
      } catch {
        case e: ZkNodeExistsException =>
          throw new IllegalArgumentException(s"Type $typeName already exists", e)
        case e: Exception =>
          throw new RuntimeException(s"Could not create path in zookeeper at $path", e)
      }
    }
    val producer = getKafkaProducer
    KafkaDataStore.writeSchema(featureType, typeName, producer)
    producer.close()
  }

  private def getKafkaProducer: Producer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put("metadata.broker.list", broker)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("request.required.acks", "1")
    props.put("producer.type", "sync")
    new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))
  }

  def getZkPath(typeName: String) = s"$zkPath/$typeName"

  val producerCache =
    CacheBuilder.newBuilder().build[ContentEntry, ContentFeatureSource](
      new CacheLoader[ContentEntry, ContentFeatureSource] {
        override def load(entry: ContentEntry) = createProducerFeatureSource(entry)
      })
  val consumerCache =
    CacheBuilder.newBuilder().build[ContentEntry, ContentFeatureSource](
      new CacheLoader[ContentEntry, ContentFeatureSource] {
        override def load(entry: ContentEntry) = createConsumerFeatureSource(entry)
      })
  val schemaCache =
    CacheBuilder.newBuilder().build(new CacheLoader[String, SimpleFeatureType] {
      override def load(k: String): SimpleFeatureType =
        resolveTopicSchema(k).getOrElse(throw new IllegalArgumentException("Unable to find schema"))
    })

  override def createFeatureSource(entry: ContentEntry) =
    if(isProducer) producerCache.get(entry)
    else consumerCache.get(entry)

  private def createProducerFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val props = new ju.Properties()
    props.put("metadata.broker.list", broker)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val kafkaProducer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))
    new KafkaProducerFeatureStore(entry, schemaCache.get(entry.getTypeName), broker, null, kafkaProducer)
  }

  private def createConsumerFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    if (createTypeNames().contains(entry.getName)) {
      val topic = entry.getTypeName
      val eb = new EventBus(topic)
      val sft = schemaCache.get(topic)
      val groupId = RandomStringUtils.randomAlphanumeric(5)
      val decoder = new AvroFeatureDecoder(sft)
      val producer = new KafkaFeatureConsumer(topic, zookeepers, groupId, decoder, eb)
      new KafkaConsumerFeatureSource(entry, sft, eb, null)
    } else null
  }

  def resolveTopicSchema(typeName: String): Option[SimpleFeatureType] = {
    val client = Consumer.create(new ConsumerConfig(buildClientProps))
    val whitelist = new Whitelist(typeName)
    val keyDecoder = new DefaultDecoder(null)
    val valueDecoder = new DefaultDecoder(null)
    val stream =
      client.createMessageStreamsByFilter(whitelist, 1, keyDecoder, valueDecoder).head

    val iter = stream.iterator()
    var schemaMsg: MessageAndMetadata[Array[Byte], Array[Byte]] = null
    while(schemaMsg == null && iter.hasNext()) {
      val msg = iter.next()
      if(msg.key() != null && !msg.key().equals("schema".getBytes())) {
        schemaMsg = msg
      }
    }
    if(schemaMsg != null) {
      val spec = schemaMsg.message()
      client.shutdown()
      Some(SimpleFeatureTypes.createType(typeName, new String(spec, StandardCharsets.UTF_8)))
    } else {
      logger.warn("Did not find a schema type")
      None
    }
    Option(zkClient.readData[String](getZkPath(typeName), true))
      .map(data => SimpleFeatureTypes.createType(typeName, data))
  }

  private def buildClientProps = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeepers)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "2000")
    props.put("zookeeper.sync.time.ms", "1000")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "smallest")
    props
  }
}

object KafkaDataStoreFactoryParams {
  val KAFKA_BROKER_PARAM = new Param("brokers", classOf[String], "Kafka broker", true)
  val ZOOKEEPERS_PARAM   = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val ZK_PATH            = new Param("zkPath", classOf[String], "Zookeeper discoverable path", false)
  val IS_PRODUCER_PARAM  = new Param("isProducer", classOf[java.lang.Boolean], "Is Producer", false, false)
}

class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.KafkaDataStoreFactoryParams._

  override def createDataStore(params: ju.Map[String, Serializable]): DataStore = {
    val broker   = KAFKA_BROKER_PARAM.lookUp(params).asInstanceOf[String]
    val zk       = ZOOKEEPERS_PARAM.lookUp(params).asInstanceOf[String]
    val zkPath   = Option(ZK_PATH.lookUp(params).asInstanceOf[String])
                     .map(_.trim)
                     .filterNot(_.isEmpty)
                     .map(p => if (p.startsWith("/")) p else "/" + p)
                     .map(p => if (p.endsWith("/")) p.substring(0, p.length - 1) else p)
                     .getOrElse("/geomesa/ds/kafka")
    val producer =
      if(IS_PRODUCER_PARAM.lookUp(params) == null) java.lang.Boolean.FALSE
      else IS_PRODUCER_PARAM.lookUp(params).asInstanceOf[java.lang.Boolean]
    new KafkaDataStore(broker, zk, zkPath, producer)
  }

  override def createNewDataStore(params: ju.Map[String, Serializable]): DataStore = ???
  override def getDescription: String = "Kafka Data Store"
  override def getParametersInfo: Array[Param] = Array(KAFKA_BROKER_PARAM, ZOOKEEPERS_PARAM)
  override def getDisplayName: String = "Kafka Data Store"
  override def canProcess(params: ju.Map[String, Serializable]): Boolean =
    KAFKA_BROKER_PARAM.lookUp(params) != null && ZOOKEEPERS_PARAM.lookUp(params) != null

  override def isAvailable: Boolean = true
  override def getImplementationHints: ju.Map[Key, _] = null
}