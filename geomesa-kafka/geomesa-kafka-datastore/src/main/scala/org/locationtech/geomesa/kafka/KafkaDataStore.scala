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
        override def load(entry: ContentEntry) = {
          val sft = schemaCache.get(entry.getTypeName)
          fsFactory(entry, sft)
        }
      })

  val schemaCache =
    CacheBuilder.newBuilder().build(new CacheLoader[String, KafkaSimpleFeatureType] {
      override def load(k: String): KafkaSimpleFeatureType =
        resolveTopicSchema(k).getOrElse(throw new IllegalArgumentException(s"Unable to find schema with name $k"))
    })

  override def createFeatureSource(entry: ContentEntry) = featureSourceCache.get(entry)
  
  def resolveTopicSchema(typeName: String): Option[KafkaSimpleFeatureType] =
    Option(zkClient.readData[String](getZkPath(typeName), true))
      .map(data => KafkaSimpleFeatureType(SimpleFeatureTypes.createType(typeName, data)))
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
  type FeatureSourceFactory = (ContentEntry, KafkaSimpleFeatureType) => ContentFeatureSource

  def producerFeatureSourceFactory(broker: String): FeatureSourceFactory = {

    val config = {
      val props = new ju.Properties()
      props.put("metadata.broker.list", broker)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")
      new ProducerConfig(props)
    }

    (entry: ContentEntry, sft: KafkaSimpleFeatureType) => {
      val kafkaProducer = new Producer[Array[Byte], Array[Byte]](config)
      new KafkaProducerFeatureStore(entry, sft, broker, null, kafkaProducer)
    }
  }

  def liveConsumerFeatureSourceFactory(kf: KafkaConsumerFactory,
                                       expiry: Boolean,
                                       expirationPeriod: Long): FeatureSourceFactory =

    (entry: ContentEntry, sft: KafkaSimpleFeatureType) =>
      new LiveKafkaConsumerFeatureSource(entry, sft, null, kf, expiry, expirationPeriod)


  def replayConsumerFeatureSourceFactory(kf: KafkaConsumerFactory, config: ReplayConfig): FeatureSourceFactory =

    (entry: ContentEntry, sft: KafkaSimpleFeatureType) =>
      new ReplayKafkaConsumerFeatureSource(entry, sft, null, kf, config)
}

/** The [[KafkaDataStore]] requires additionally configuration to be included in the user data of the
  * [[SimpleFeatureType]], including the name of the Kafka topic which is required.  Optionally,
  * [[ReplayConfig]] may be included.
  *
  * This class allows that additional configuration to be stored and retrieved.
  *
  * @param sft the [[SimpleFeatureType]] with additional user data
  */
case class KafkaSimpleFeatureType(sft: SimpleFeatureType) extends AnyRef {

  /** @return the name of the Kafka topic; defaults to the name of the [[SimpleFeatureType]] if not set
    */
  def topic: String = sft.getTypeName

  /** @param name the name of the Kafka topic
    *
    * @return a copy of ``this`` containing a copy of the ``sft`` with the given topic ``name`` set in the
    *         user data replacing any previously set topic name
    */
  def topic(name: String): KafkaSimpleFeatureType = ???


  /** @return the [[ReplayConfig]], if any has been set
    */
  def replayConfig: Option[ReplayConfig] = ???

  /** @param config the replay configuration
    *
    * @return a copy of ``this`` containing a copy of the ``sft`` with the given replay ``config`` set in
    *         the user data replacing any previously set replay configuration
    */
  def replayConfig(config: ReplayConfig): KafkaSimpleFeatureType = ???


  override def toString: String =
    s"KafkaSimpleFeatureType: typeName=${sft.getTypeName}; topic=$topic; replayConfig=$replayConfig"
}

/** Standard factory for [[KafkaDataStore]] allows the creation of either a Producer DS or a Live
  * Consumer DS.
  */
class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.KafkaDataStore._
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

    val partitions       = Option(TOPIC_PARTITIONS.lookUp(params)).map(_.toString.toInt).getOrElse(1)
    val replication      = Option(TOPIC_REPLICATION.lookUp(params)).map(_.toString.toInt).getOrElse(1)

    val fsFactory = createFeatureSourceFactory(broker, zk, params)

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