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
import kafka.producer.{Producer, ProducerConfig}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.kafka.KafkaDataStore.FeatureSourceFactory

class KafkaDataStore(override val zookeepers: String,
                     override val zkPath: String,
                     override val partitions: Int,
                     override val replication: Int,
                     fsFactory: FeatureSourceFactory)
  extends ContentDataStore
  with KafkaDataStoreSchemaManager
  with Logging {

  override def createTypeNames() = getNames()

  val featureSourceCache =
    CacheBuilder.newBuilder().build[ContentEntry, ContentFeatureSource](
      new CacheLoader[ContentEntry, ContentFeatureSource] {
        override def load(entry: ContentEntry) = {
          val sft = getFeatureConfig(entry.getTypeName)
          fsFactory(entry, sft)
        }
      })

  override def createFeatureSource(entry: ContentEntry) = featureSourceCache.get(entry)

}

object KafkaDataStoreFactoryParams {
  // general params
  val KAFKA_BROKER_PARAM = new Param("brokers", classOf[String], "Kafka broker", true)
  val ZOOKEEPERS_PARAM   = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val ZK_PATH            = new Param("zkPath", classOf[String], "Zookeeper discoverable path", false)
  val TOPIC_PARTITIONS   = new Param("partitions", classOf[Integer], "Number of partitions to use in kafka topics", false)
  val TOPIC_REPLICATION  = new Param("replication", classOf[Integer], "Replication factor to use in kafka topics", false)

  // producer or consumer?
  val IS_PRODUCER_PARAM  = new Param("isProducer", classOf[java.lang.Boolean], "Is Producer", false, false)

  // consumer params
  val EXPIRY             = new Param("expiry", classOf[java.lang.Boolean], "Expiry", false, false)
  val EXPIRATION_PERIOD  = new Param("expirationPeriod", classOf[java.lang.Long], "Expiration Period in milliseconds", false)
}

object KafkaDataStore {
  type FeatureSourceFactory = (ContentEntry, KafkaFeatureConfig) => ContentFeatureSource

  def producerFeatureSourceFactory(broker: String): FeatureSourceFactory = {

    val config = {
      val props = new ju.Properties()
      props.put("metadata.broker.list", broker)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")
      new ProducerConfig(props)
    }

    (entry: ContentEntry, fc: KafkaFeatureConfig) => {
      val kafkaProducer = new Producer[Array[Byte], Array[Byte]](config)
      new KafkaProducerFeatureStore(entry, fc.sft, fc.topic, broker, kafkaProducer)
    }
  }

  def consumerFeatureSourceFactory(kf: KafkaConsumerFactory,
                                   expiry: Boolean,
                                   expirationPeriod: Long): FeatureSourceFactory =

    (entry: ContentEntry, fc: KafkaFeatureConfig) => fc.replayConfig match {
      case None =>
        new LiveKafkaConsumerFeatureSource(entry, fc.sft, fc.topic, kf, expiry, expirationPeriod)

      case Some(rc) =>
        new ReplayKafkaConsumerFeatureSource(entry, fc.sft, fc.topic, kf, rc)
    }
}

/** A [[DataStoreFactorySpi]] to create a [[KafkaDataStore]] in either producer or consumer mode */
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

      consumerFeatureSourceFactory(kf, expiry, expirationPeriod)
    }
  }

  override def createNewDataStore(params: ju.Map[String, Serializable]): DataStore =
    throw new UnsupportedOperationException

  override def getDisplayName: String = "Kafka Data Store"
  override def getDescription: String = "Kafka Data Store"

  override def getParametersInfo: Array[Param] = Array(KAFKA_BROKER_PARAM, ZOOKEEPERS_PARAM)

  override def canProcess(params: ju.Map[String, Serializable]): Boolean =
    params.containsKey(KAFKA_BROKER_PARAM.key) && params.containsKey(ZOOKEEPERS_PARAM.key)

  override def isAvailable: Boolean = true
  override def getImplementationHints: ju.Map[Key, _] = null
}
