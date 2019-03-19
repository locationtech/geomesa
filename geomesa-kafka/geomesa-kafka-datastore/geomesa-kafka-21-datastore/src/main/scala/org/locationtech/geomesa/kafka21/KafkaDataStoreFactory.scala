/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka21

import java.awt.RenderingHints.Key
import java.io.Serializable
import java.util.Collections

import com.google.common.collect.Lists
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper
import org.locationtech.geomesa.kafka21.KafkaDataStore.{ConsumerConfig, ProducerConfig}
import org.locationtech.geomesa.utils.text.Suffixes


/** A [[DataStoreFactorySpi]] to create a [[KafkaDataStore]] in either producer or consumer mode */
class KafkaDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  import KafkaDataStoreFactoryParams._

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    val broker = KAFKA_BROKER_PARAM.lookUp(params).asInstanceOf[String]
    val zoos = ZOOKEEPERS_PARAM.lookUp(params).asInstanceOf[String]
    val zkPath = KafkaDataStoreHelper.cleanZkPath(ZK_PATH.lookUp(params).asInstanceOf[String])

    val partitions = Option(TOPIC_PARTITIONS.lookUp(params)).map(_.toString.toInt).getOrElse(1)
    val replication = Option(TOPIC_REPLICATION.lookUp(params)).map(_.toString.toInt).getOrElse(1)

    val isProducer = Option(IS_PRODUCER_PARAM.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)

    val config: Either[ConsumerConfig, ProducerConfig] = if (isProducer) {
      Right(ProducerConfig(KafkaDataStore.parseConfig(Option(PRODUCER_CFG_PARAM.lookUp(params).asInstanceOf[String]))))
    } else {
      val expirationPeriod: Option[Long] = Option(EXPIRATION_PERIOD.lookUp(params)).map(_.toString.toLong).filter(_ > 0)
      val consistencyCheck: Option[Long] = Option(CONSISTENCY_CHECK.lookUp(params)).map(_.toString.toLong).filter(_ > 0)
      val cleanUpCache: Boolean = Option(CLEANUP_LIVE_CACHE.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
      val cacheCleanUpPeriod: Long = {
        Option(CACHE_CLEANUP_PERIOD.lookUp(params).asInstanceOf[String]) match {
          case Some(duration) => Suffixes.Time.millis(duration).getOrElse {
            logger.warn("Unable to parse Cache Cleanup Period. Using default of 10000")
            10000
          }
          case None => 10000
        }
      }
      val useCQCache: Boolean = Option(USE_CQ_LIVE_CACHE.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
      val monitor: Boolean = Option(COLLECT_QUERY_STAT.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
      val consumerConfig = KafkaDataStore.parseConfig(Option(CONSUMER_CFG_PARAM.lookUp(params).asInstanceOf[String]))
      Option(AUTO_OFFSET_RESET.lookUp(params).asInstanceOf[String]).foreach { value =>
        val deprecated = value match {
          case v if v.equalsIgnoreCase("largest") => "latest"
          case v if v.equalsIgnoreCase("smallest") => "earliest"
          case v => v
        }
        consumerConfig.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, deprecated)
      }
      Left(ConsumerConfig(consumerConfig, expirationPeriod, consistencyCheck, cleanUpCache, cacheCleanUpPeriod, useCQCache, monitor))
    }

    val ds = new KafkaDataStore(broker, zoos, zkPath, partitions, replication, config)
    Option(NAMESPACE_PARAM.lookUp(params).asInstanceOf[String]).foreach(ds.setNamespaceURI)
    ds
  }

  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore =
    throw new UnsupportedOperationException

  override def getDisplayName: String = "Kafka (GeoMesa)"

  override def getDescription: String = "Apache Kafka\u2122 distributed messaging queue"

  override def getParametersInfo: Array[Param] =
    Array(
      KAFKA_BROKER_PARAM,
      ZOOKEEPERS_PARAM,
      ZK_PATH,
      EXPIRATION_PERIOD,
      CONSISTENCY_CHECK,
      CLEANUP_LIVE_CACHE,
      CACHE_CLEANUP_PERIOD,
      USE_CQ_LIVE_CACHE,
      TOPIC_PARTITIONS,
      TOPIC_REPLICATION,
      PRODUCER_CFG_PARAM,
      CONSUMER_CFG_PARAM,
      COLLECT_QUERY_STAT,
      AUTO_OFFSET_RESET,
      NAMESPACE_PARAM)

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    params.containsKey(KAFKA_BROKER_PARAM.key) && params.containsKey(ZOOKEEPERS_PARAM.key)

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[Key, _] = null
}

object KafkaDataStoreFactoryParams {
  val KAFKA_BROKER_PARAM = new Param("brokers", classOf[String], "Kafka broker", true)
  val ZOOKEEPERS_PARAM = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val ZK_PATH = new Param("zkPath", classOf[String], "Zookeeper discoverable path", false, KafkaDataStoreHelper.DefaultZkPath)
  val NAMESPACE_PARAM = new Param("namespace", classOf[String], "Namespace", false)
  val PRODUCER_CFG_PARAM = new Param("producerConfig", classOf[String], "Configuration options for kafka producer, in Java properties format. Passed directly to kafka", false, null, Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))
  val CONSUMER_CFG_PARAM = new Param("consumerConfig", classOf[String], "Configuration options for kafka consumer, in Java properties format. Passed directly to kafka", false, null, Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))
  val TOPIC_PARTITIONS = new Param("partitions", classOf[Integer], "Number of partitions to use in kafka topics", false)
  val TOPIC_REPLICATION = new Param("replication", classOf[Integer], "Replication factor to use in kafka topics", false)
  val IS_PRODUCER_PARAM = new Param("isProducer", classOf[java.lang.Boolean], "Is Producer", false, false)
  val EXPIRATION_PERIOD = new Param("expirationPeriod", classOf[java.lang.Long], "Features will be auto-dropped (expired) after this delay in milliseconds. Leave blank or use -1 to not drop features.", false)
  val CONSISTENCY_CHECK = new Param("consistencyCheck", classOf[java.lang.Long], "Milliseconds between checking the feature cache for consistency. Leave blank or use -1 to not check for consistency.", false)
  val CLEANUP_LIVE_CACHE   = new Param("cleanUpCache", classOf[java.lang.Boolean], "Run a thread to clean up the live feature cache if set to true. False by default. use 'cleanUpCachePeriod' to configure the length of time between cache cleanups. Every second by default", false)
  val CACHE_CLEANUP_PERIOD = new Param("cleanUpCachePeriod", classOf[String], "Configure the time period between cache cleanups. Default is every ten seconds. This parameter is not used if 'cleanUpCache' is false.", false, "10s")
  val USE_CQ_LIVE_CACHE  = new Param("useCQCache", classOf[java.lang.Boolean], "Use CQEngine-based implementation of live feature cache. False by default.", false, false)
  val COLLECT_QUERY_STAT = new Param("collectQueryStats", classOf[java.lang.Boolean], "Enable monitoring stats for feature store.", false)
  val AUTO_OFFSET_RESET = new Param("autoOffsetReset", classOf[java.lang.String], "What offset to reset to when there is no initial offset in ZooKeeper or if an offset is out of range. Default is latest.", false, "latest", Collections.singletonMap(Parameter.OPTIONS, Lists.newArrayList("latest","earliest")))
}
