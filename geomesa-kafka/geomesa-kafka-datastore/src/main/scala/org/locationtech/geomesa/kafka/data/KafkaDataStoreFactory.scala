/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.awt.RenderingHints
import java.io.{Serializable, StringReader}
import java.util.{Collections, Properties}

import com.github.benmanes.caffeine.cache.Ticker
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStore.KafkaDataStoreConfig
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams.{Brokers, ZkPath, Zookeepers}
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, NoOpAuditProvider}

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): KafkaDataStore =
    createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): KafkaDataStore =
    new KafkaDataStore(KafkaDataStoreFactory.buildConfig(params))

  override def getDisplayName: String = KafkaDataStoreFactory.DisplayName

  override def getDescription: String = KafkaDataStoreFactory.Description

  // note: we don't return producer configs, as they would not be used in geoserver
  override def getParametersInfo: Array[Param] =
    Array(
      Brokers,
      Zookeepers,
      ZkPath,
      Authorizations,
      CacheExpiry,
      CacheCleanup,
      ConsumerConfig,
      CqEngineCache,
      ConsumeEarliest,
      AuditQueries,
      LooseBBox
    )

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    KafkaDataStoreFactory.canProcess(params)

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object KafkaDataStoreFactory {

  val DisplayName = "Kafka (GeoMesa)"
  val Description = "Apache Kafka\u2122 distributed log"

  val DefaultZkPath: String = "geomesa/ds/kafka"

  def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    params.containsKey(Brokers.key) && params.containsKey(Zookeepers.key)

  def buildConfig(params: java.util.Map[String, Serializable]): KafkaDataStoreConfig = {
    import KafkaDataStoreFactoryParams._
    import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.RichParam

    val catalog = createZkNamespace(params)
    val brokers = checkBrokerPorts(Brokers.lookup[String](params))
    val zookeepers = Zookeepers.lookup[String](params)

    val partitions = TopicPartitions.lookupWithDefault[Int](params)
    val replication = TopicReplication.lookupWithDefault[Int](params)

    val consumers = ConsumerCount.lookupWithDefault[Int](params)

    val producerConfig = ProducerConfig.lookupOpt[String](params).map(toProperties).getOrElse(new Properties)
    val consumerConfig = ConsumerConfig.lookupOpt[String](params).map(toProperties).getOrElse(new Properties)

    val consumeFromBeginning = ConsumeEarliest.lookupWithDefault[Boolean](params)

    val cacheExpiry = CacheExpiry.lookupOpt[String](params).map(toDuration).getOrElse(Duration.Inf)
    val cacheCleanup = CacheCleanup.lookupOpt[String](params).map(toDuration).getOrElse(Duration("30s"))
    val ticker = CacheTicker.lookupOpt[Ticker](params).getOrElse(Ticker.systemTicker())

    val cqEngine = CqEngineCache.lookupWithDefault[Boolean](params)

    val looseBBox = LooseBBox.lookupWithDefault[Boolean](params)

    val audit = if (!AuditQueries.lookupWithDefault[Boolean](params)) { None } else {
      Some((AuditLogger, buildAuditProvider(params), "kafka"))
    }
    val authProvider = buildAuthProvider(params)

    KafkaDataStoreConfig(catalog, brokers, zookeepers, consumers, partitions, replication,
      producerConfig, consumerConfig, consumeFromBeginning, cacheExpiry, cacheCleanup, ticker, cqEngine,
      looseBBox, authProvider, audit)
  }

  private def buildAuthProvider(params: java.util.Map[String, Serializable]): AuthorizationsProvider = {
    import KafkaDataStoreFactoryParams.Authorizations
    import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.RichParam
    // get the auth params passed in as a comma-delimited string
    val auths = Authorizations.lookupOpt[String](params).map(_.split(",").filterNot(_.isEmpty)).getOrElse(Array.empty)
    security.getAuthorizationsProvider(params, auths)
  }

  private def buildAuditProvider(params: java.util.Map[String, Serializable]): AuditProvider =
    Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider)

  /**
    * Gets up a zk path parameter - trims, removes leading/trailing "/" if needed
    *
    * @param params data store params
    * @return
    */
  private [data] def createZkNamespace(params: java.util.Map[String, Serializable]): String = {
    import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.RichParam
    ZkPath.lookupOpt[String](params)
        .map(_.trim)
        .filterNot(_.isEmpty)
        .map(p => if (p.startsWith("/")) { p.substring(1).trim } else { p })  // leading '/'
        .map(p => if (p.endsWith("/")) { p.substring(0, p.length - 1).trim } else { p })  // trailing '/'
        .filterNot(_.isEmpty)
        .getOrElse(DefaultZkPath)
  }

  private def checkBrokerPorts(brokers: String): String = {
    if (brokers.indexOf(':') != -1) { brokers } else {
      try { brokers.split(",").map(b => s"${b.trim}:9092").mkString(",") } catch {
        case NonFatal(e) => brokers
      }
    }
  }

  private def toProperties(param: String): Properties = {
    val props = new Properties
    props.load(new StringReader(param))
    props
  }

  private def toDuration(param: String): Duration = {
    try { Duration(param) } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Couldn't parse duration parameter: $param", e)
    }
  }

  object KafkaDataStoreFactoryParams {
    val Brokers          = new Param("kafka.brokers", classOf[String], "Kafka brokers", true)
    val Zookeepers       = new Param("kafka.zookeepers", classOf[String], "Kafka zookeepers", true)
    val ZkPath           = new Param("kafka.zk.path", classOf[String], "Zookeeper discoverable path (namespace)", false, s"$DefaultZkPath")
    val ProducerConfig   = new Param("kafka.producer.config", classOf[String], "Configuration options for kafka producer, in Java properties format. See http://kafka.apache.org/documentation.html#producerconfigs", false, null, Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))
    val ConsumerConfig   = new Param("kafka.consumer.config", classOf[String], "Configuration options for kafka consumer, in Java properties format. See http://kafka.apache.org/documentation.html#newconsumerconfigs", false, null, Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))
    val ConsumeEarliest  = new Param("kafka.consumer.from-beginning", classOf[java.lang.Boolean], "Start reading from the beginning of the topic (vs ignore old messages)", false, false)
    val TopicPartitions  = new Param("kafka.topic.partitions", classOf[Integer], "Number of partitions to use in kafka topics", false, 1)
    val TopicReplication = new Param("kafka.topic.replication", classOf[Integer], "Replication factor to use in kafka topics", false, 1)
    val ConsumerCount    = new Param("kafka.consumer.count", classOf[Integer], "Number of kafka consumers used per feature type. Set to 0 to disable consuming (i.e. producer only)", false, Int.box(1))
    val CacheExpiry      = new Param("kafka.cache.expiry", classOf[String], "Features will be expired after this delay", false)
    val CacheCleanup     = new Param("kafka.cache.cleanup", classOf[String], "Run a thread to clean expired features from the cache (vs cleanup during reads and writes)", false)
    val CacheTicker      = new Param("kafka.cache.ticker", classOf[Ticker], "Ticker to use for expiring/cleaning feature cache", false)
    val CqEngineCache    = new Param("kafka.cache.cqengine", classOf[java.lang.Boolean], "Use CQEngine-based implementation of live feature cache", false, false)
    val LooseBBox        = GeoMesaDataStoreFactory.LooseBBoxParam
    val AuditQueries     = GeoMesaDataStoreFactory.AuditQueriesParam
    val Authorizations   = org.locationtech.geomesa.security.AuthsParam
  }
}
