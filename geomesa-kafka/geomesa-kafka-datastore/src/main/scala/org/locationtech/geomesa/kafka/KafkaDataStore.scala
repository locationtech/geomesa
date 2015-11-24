/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.awt.RenderingHints.Key
import java.io.Serializable
import java.{util => ju}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.slf4j.Logging
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

  kds =>

  override def createTypeNames() = getNames()

  private val featureSourceCache =
    CacheBuilder.newBuilder().build[ContentEntry, ContentFeatureSource](
      new CacheLoader[ContentEntry, ContentFeatureSource] {
        override def load(entry: ContentEntry) = {
          fsFactory(entry, kds)
        }
      })

  override def createFeatureSource(entry: ContentEntry) = featureSourceCache.get(entry)

  override def dispose(): Unit = {
    super[ContentDataStore].dispose()
    super[KafkaDataStoreSchemaManager].dispose()
  }
}

object KafkaDataStoreFactoryParams {
  val KAFKA_BROKER_PARAM = new Param("brokers", classOf[String], "Kafka broker", true)
  val ZOOKEEPERS_PARAM   = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val ZK_PATH            = new Param("zkPath", classOf[String], "Zookeeper discoverable path", false,  KafkaDataStoreHelper.DefaultZkPath)
  val TOPIC_PARTITIONS   = new Param("partitions", classOf[Integer], "Number of partitions to use in kafka topics", false)
  val TOPIC_REPLICATION  = new Param("replication", classOf[Integer], "Replication factor to use in kafka topics", false)
  val IS_PRODUCER_PARAM  = new Param("isProducer", classOf[java.lang.Boolean], "Is Producer", false, false)
  val EXPIRATION_PERIOD  = new Param("expirationPeriod", classOf[java.lang.Long], "Features will be auto-dropped (expired) after this delay in milliseconds. Leave blank or use -1 to not drop features.", false)
  val CLEANUP_LIVE_CACHE = new Param("cleanUpCache", classOf[java.lang.Boolean], "Run a thread to clean up the live feature cache every second if set to true. False by default.", false)
}

object KafkaDataStore {
  type FeatureSourceFactory = (ContentEntry, KafkaDataStoreSchemaManager) => ContentFeatureSource
}

/** A [[DataStoreFactorySpi]] to create a [[KafkaDataStore]] in either producer or consumer mode */
class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.KafkaDataStore._
  import org.locationtech.geomesa.kafka.KafkaDataStoreFactoryParams._

  override def createDataStore(params: ju.Map[String, Serializable]): DataStore = {
    val broker = KAFKA_BROKER_PARAM.lookUp(params).asInstanceOf[String]
    val zkHost = ZOOKEEPERS_PARAM.lookUp(params).asInstanceOf[String]
    val zkPath = KafkaDataStoreHelper.cleanZkPath(ZK_PATH.lookUp(params).asInstanceOf[String])

    val partitions  = Option(TOPIC_PARTITIONS.lookUp(params)).map(_.toString.toInt).getOrElse(1)
    val replication = Option(TOPIC_REPLICATION.lookUp(params)).map(_.toString.toInt).getOrElse(1)

    val fsFactory = createFeatureSourceFactory(broker, zkHost, params)

    new KafkaDataStore(zkHost, zkPath, partitions, replication, fsFactory)
  }

  def createFeatureSourceFactory(brokers: String,
                                 zk: String,
                                 params: ju.Map[String, Serializable]): FeatureSourceFactory = {

    val isProducer = Option(IS_PRODUCER_PARAM.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)

    if (isProducer) {
      KafkaProducerFeatureStoreFactory(brokers)
    } else {
      KafkaConsumerFeatureSourceFactory(brokers, zk, params)
    }
  }

  override def createNewDataStore(params: ju.Map[String, Serializable]): DataStore =
    throw new UnsupportedOperationException

  override def getDisplayName: String = "Kafka Data Store"
  override def getDescription: String = "Kafka Data Store"

  override def getParametersInfo: Array[Param] =
    Array(KAFKA_BROKER_PARAM, ZOOKEEPERS_PARAM, ZK_PATH, EXPIRATION_PERIOD, CLEANUP_LIVE_CACHE, TOPIC_PARTITIONS, TOPIC_REPLICATION)

  override def canProcess(params: ju.Map[String, Serializable]): Boolean =
    params.containsKey(KAFKA_BROKER_PARAM.key) && params.containsKey(ZOOKEEPERS_PARAM.key)

  override def isAvailable: Boolean = true
  override def getImplementationHints: ju.Map[Key, _] = null
}
