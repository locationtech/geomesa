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
import java.{util => ju}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.slf4j.Logging
import kafka.admin.AdminUtils
import kafka.producer.{Producer, ProducerConfig}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.geotools.feature.NameImpl
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.kafka.KafkaDataStore.FeatureSourceFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

class KafkaDataStore(zookeepers: String,
                     zkPath: String,
                     partitions: Int,
                     replication: Int,
                     fsFactory: FeatureSourceFactory) extends ContentDataStore with Logging {

  ds =>

  import scala.collection.JavaConversions._

  val zkClient = {
    // zkStringSerializer is required - otherwise topics won't be created correctly
    val ret = new ZkClient(zookeepers, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    if (!ret.exists(zkPath)) {
      try {
        ret.createPersistent(zkPath, true)
      } catch {
        case e: ZkNodeExistsException => // it's ok, something else created before we could
        case e: Exception => throw new RuntimeException(s"Could not create path in zookeeper at $zkPath", e)
      }
    }
    ret
  }

  override def createTypeNames() =
    zkClient.getChildren(zkPath).map(new NameImpl(_))

  override def createSchema(featureType: SimpleFeatureType) = {
    val typeName = featureType.getTypeName
    val path = getZkPath(typeName)

    if (zkClient.exists(path)) {
      throw new IllegalArgumentException(s"Type $typeName already exists")
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

    AdminUtils.createTopic(zkClient, typeName, partitions, replication)
  }

  private def getZkPath(typeName: String) = s"$zkPath/$typeName"

  val featureSourceCache =
    CacheBuilder.newBuilder().build[ContentEntry, ContentFeatureSource](
      new CacheLoader[ContentEntry, ContentFeatureSource] {
        override def load(entry: ContentEntry) = fsFactory(ds, entry)
      })

  val schemaCache =
    CacheBuilder.newBuilder().build(new CacheLoader[String, SimpleFeatureType] {
      override def load(k: String): SimpleFeatureType =
        resolveTopicSchema(k).getOrElse(throw new IllegalArgumentException("Unable to find schema"))
    })

  override def createFeatureSource(entry: ContentEntry) = featureSourceCache.get(entry)
  
  def resolveTopicSchema(typeName: String): Option[SimpleFeatureType] =
    Option(zkClient.readData[String](getZkPath(typeName), true))
      .map(data => SimpleFeatureTypes.createType(typeName, data))
}

object KafkaDataStoreFactoryParams {
  // general
  val KAFKA_BROKER_PARAM = new Param("brokers", classOf[String], "Kafka broker", true)
  val ZOOKEEPERS_PARAM   = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val ZK_PATH            = new Param("zkPath", classOf[String], "Zookeeper discoverable path", false)
  val TOPIC_PARTITIONS   = new Param("partitions", classOf[Integer], "Number of partitions to use in kafka topics", false)
  val TOPIC_REPLICATION  = new Param("replication", classOf[Integer], "Replication factor to use in kafka topics", false)

  // producer or live consumer?
  val IS_PRODUCER_PARAM  = new Param("isProducer", classOf[java.lang.Boolean], "Is Producer", false, false)

  // live consumer
  val EXPIRY             = new Param("expiry", classOf[java.lang.Boolean], "Expiry", false, false)
  val EXPIRATION_PERIOD  = new Param("expirationPeriod", classOf[java.lang.Long], "Expiration Period in milliseconds", false)
}

object ReplayKafkaDataStoreFactoryParams {
  // replay consumer
  val REPLAY_START_TIME  = new Param("replayStart", classOf[java.lang.Long],
                                      "Lower bound on replay window, UTC epoic", true)
  val REPLAY_END_TIME    = new Param("replayEnd", classOf[java.lang.Long],
                                      "Upper bound on replay window, UTC epoic", true)
  val REPLAY_READ_BEHIND = new Param("replayReadBehind", classOf[java.lang.Long],
                                      "Milliseconds of log to log to read before requested read time", true)
}

object KafkaDataStore {
  type FeatureSourceFactory = (KafkaDataStore, ContentEntry) => ContentFeatureSource

  def producerFeatureSourceFactory(broker: String): FeatureSourceFactory =

    (ds: KafkaDataStore, entry: ContentEntry) => {
      val props = new ju.Properties()
      props.put("metadata.broker.list", broker)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")
      val kafkaProducer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))
      new KafkaProducerFeatureStore(entry, ds.schemaCache.get(entry.getTypeName), broker, null, kafkaProducer)
    }

  def liveConsumerFeatureSourceFactory(kf: KafkaConsumerFactory,
                                       expiry: Boolean,
                                       expirationPeriod: Long): FeatureSourceFactory =

    (ds: KafkaDataStore, entry: ContentEntry) => {
      if (ds.createTypeNames().contains(entry.getName)) {
        val topic = entry.getTypeName
        val sft = ds.schemaCache.get(topic)
        new LiveKafkaConsumerFeatureSource(entry, sft, null, topic, kf, expiry, expirationPeriod)
      } else {
        null
      }
  }

  def replayConsumerFeatureSourceFactory(kf: KafkaConsumerFactory, config: ReplayConfig): FeatureSourceFactory =

    (ds: KafkaDataStore, entry: ContentEntry) => {
      if (ds.createTypeNames().contains(entry.getName)) {
        val topic = entry.getTypeName
        val sft = ds.schemaCache.get(topic)
        new ReplayKafkaConsumerFeatureSource(entry, sft, null, topic, kf, config)
      } else {
        null
      }
    }
}

/** Standard factory for [[KafkaDataStore]] allows the creation of either a Producer DS or a Live
  * Consumer DS.
  */
class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.KafkaDataStore._
  import org.locationtech.geomesa.kafka.KafkaDataStoreFactoryParams._

  override def createDataStore(params: ju.Map[String, Serializable]): DataStore = {
    val brokers = KAFKA_BROKER_PARAM.lookUp(params).asInstanceOf[String]
    val zk = ZOOKEEPERS_PARAM.lookUp(params).asInstanceOf[String]
    val zkPath = Option(ZK_PATH.lookUp(params).asInstanceOf[String])
                     .map(_.trim)
                     .filterNot(_.isEmpty)
                     .map(p => if (p.startsWith("/")) p else "/" + p)
                     .map(p => if (p.endsWith("/")) p.substring(0, p.length - 1) else p)
                     .getOrElse("/geomesa/ds/kafka")

    val partitions = Option(TOPIC_PARTITIONS.lookUp(params)).map(_.toString.toInt).getOrElse(1)
    val replication = Option(TOPIC_REPLICATION.lookUp(params)).map(_.toString.toInt).getOrElse(1)

    val fsFactory = createFeatureSourceFactory(brokers, zk, params)

    new KafkaDataStore(zk, zkPath, partitions, replication, fsFactory)
  }

  def createFeatureSourceFactory(brokers: String,
                                 zk: String,
                                 params: ju.Map[String, Serializable]): FeatureSourceFactory = {

    val isProducer = Option(IS_PRODUCER_PARAM.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)

    if (isProducer) {
      producerFeatureSourceFactory(brokers)
    } else {
      val kf = new KafkaConsumerFactory(brokers, zk)

      val expiry           = Option(EXPIRY.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
      val expirationPeriod = Option(EXPIRATION_PERIOD.lookUp(params)).map(_.toString.toLong).getOrElse(0L)

      liveConsumerFeatureSourceFactory(kf, expiry, expirationPeriod)
    }
  }

  override def createNewDataStore(params: ju.Map[String, Serializable]): DataStore = ???
  override def getDescription: String = "Kafka Data Store"
  override def getParametersInfo: Array[Param] = Array(KAFKA_BROKER_PARAM, ZOOKEEPERS_PARAM)
  override def getDisplayName: String = "Kafka Data Store"
  override def canProcess(params: ju.Map[String, Serializable]): Boolean =
    params.containsKey(KAFKA_BROKER_PARAM.key) && params.containsKey(ZOOKEEPERS_PARAM.key)

  override def isAvailable: Boolean = true
  override def getImplementationHints: ju.Map[Key, _] = null
}


/** A [[KafkaDataStore]] factory for creating a Replay Consumer DS.
  *
  */
class ReplayKafkaDataStoreFactory extends KafkaDataStoreFactory {

  import org.locationtech.geomesa.kafka.KafkaDataStore._
  import org.locationtech.geomesa.kafka.KafkaDataStoreFactoryParams._
  import org.locationtech.geomesa.kafka.ReplayKafkaDataStoreFactoryParams._

  override def createFeatureSourceFactory(brokers: String,
                                          zk: String,
                                          params: ju.Map[String, Serializable]): FeatureSourceFactory = {

    val start = new Instant(REPLAY_START_TIME.lookUp(params).asInstanceOf[Long])
    val end = new Instant(REPLAY_END_TIME.lookUp(params).asInstanceOf[Long])
    val readBehind = Duration.millis(REPLAY_READ_BEHIND.lookUp(params).asInstanceOf[Long])

    val kf = new KafkaConsumerFactory(brokers, zk)
    val replayConfig = new ReplayConfig(start, end, readBehind)

    replayConsumerFeatureSourceFactory(kf, replayConfig)
  }

  override def getDisplayName: String = "Replay Kafka Data Store"
  override def getDescription: String = "Query a Kafka Data Store at a specific point in history."

  override def getParametersInfo: Array[Param] =
    Array(KAFKA_BROKER_PARAM, ZOOKEEPERS_PARAM, REPLAY_START_TIME, REPLAY_END_TIME, REPLAY_READ_BEHIND)

  override def canProcess(params: ju.Map[String, Serializable]): Boolean =
    getParametersInfo.forall(p => params.containsKey(p.key))
}

object ReplayKafkaDataStoreFactory {

  def props(brokers: String,
            zookeepers: String,
            startTime: Instant,
            endTime: Instant,
            readBehind: Duration): ju.Map[String, Serializable] = {

    import org.locationtech.geomesa.kafka.KafkaDataStoreFactoryParams._
    import org.locationtech.geomesa.kafka.ReplayKafkaDataStoreFactoryParams._

    import scala.collection.JavaConverters._

    Map(
      KAFKA_BROKER_PARAM.key -> brokers,
      ZOOKEEPERS_PARAM.key -> zookeepers,
      REPLAY_START_TIME.key -> startTime.getMillis.asInstanceOf[Serializable],
      REPLAY_END_TIME.key -> endTime.getMillis.asInstanceOf[Serializable],
      REPLAY_READ_BEHIND.key -> readBehind.getMillis.asInstanceOf[Serializable]
    ).asJava
  }

}