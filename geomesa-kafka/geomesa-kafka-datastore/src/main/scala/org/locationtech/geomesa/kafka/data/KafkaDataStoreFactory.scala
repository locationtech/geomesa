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
import java.util.Properties

import com.github.benmanes.caffeine.cache.Ticker
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceParams
import org.locationtech.geomesa.kafka.data.KafkaDataStore.KafkaDataStoreConfig
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams.{Brokers, ZkPath, Zookeepers}
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

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
      CacheConsistency,
      ConsumerConfig,
      CqEngineCache,
      ConsumeEarliest,
      AuditQueries,
      LooseBBox,
      NamespaceParam
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
    Brokers.exists(params) && Zookeepers.exists(params)

  def buildConfig(params: java.util.Map[String, Serializable]): KafkaDataStoreConfig = {
    import KafkaDataStoreFactoryParams._

    val catalog = createZkNamespace(params)
    val brokers = checkBrokerPorts(Brokers.lookup(params))
    val zookeepers = Zookeepers.lookup(params)

    val partitions = TopicPartitions.lookup(params)
    val replication = TopicReplication.lookup(params)

    val consumers = {
      if (params.containsKey("isProducer")) {
        ConsumerCount.deprecationWarning("isProducer")
        // override default to 0
        if (!ConsumerCount.exists(params)) { 0 } else {
          ConsumerCount.lookup(params).intValue
        }
      } else {
        ConsumerCount.lookup(params).intValue
      }
    }

    val producerConfig = ProducerConfig.lookupOpt(params).map(toProperties).getOrElse(new Properties)
    val consumerConfig = ConsumerConfig.lookupOpt(params).map(toProperties).getOrElse(new Properties)

    val consumeFromBeginning = {
      val deprecated = if (ConsumeEarliest.exists(params) || !params.containsKey("autoOffsetReset")) { None } else {
        ConsumeEarliest.deprecationWarning("autoOffsetReset")
        Some("earliest".equalsIgnoreCase(new GeoMesaParam[String]("autoOffsetReset").lookup(params)))
      }
      deprecated.getOrElse(ConsumeEarliest.lookup(params).booleanValue)
    }

    val cacheExpiry = {
      val deprecated = if (CacheExpiry.exists(params) || !params.containsKey("expirationPeriod")) { None } else {
        CacheExpiry.deprecationWarning("expirationPeriod")
        new GeoMesaParam[java.lang.Long]("expirationPeriod").lookupOpt(params).filter(_ > 0).map(Duration(_, "ms"))
      }
      deprecated.orElse(CacheExpiry.lookupOpt(params)).getOrElse(Duration.Inf)
    }
    val cacheCleanup = {
      val deprecated = if (CacheCleanup.exists(params) || !params.containsKey("cleanUpCache")) { None } else {
        val param = new GeoMesaParam[java.lang.Boolean]("cleanUpCache", default = false)
        if (!param.lookup(params)) { Some(Duration.Inf) } else {
          Some(Duration(new GeoMesaParam[String]("cleanUpCachePeriod", default = "10s").lookup(params)))
        }
      }
      deprecated.getOrElse(CacheCleanup.lookup(params))
    }
    val cacheConsistency = {
      val deprecated = if (CacheConsistency.exists(params) || !params.containsKey("consistencyCheck")) { None } else {
        CacheConsistency.deprecationWarning("consistencyCheck")
        new GeoMesaParam[java.lang.Long]("consistencyCheck").lookupOpt(params).filter(_ > 0).map(Duration(_, "ms"))
      }
      deprecated.orElse(CacheConsistency.lookupOpt(params)).getOrElse(Duration.Inf)
    }
    val ticker = CacheTicker.lookupOpt(params).getOrElse(Ticker.systemTicker())

    val cqEngine = CqEngineCache.lookup(params)
    val looseBBox = LooseBBox.lookup(params)

    val audit = if (!AuditQueries.lookup(params)) { None } else {
      Some((AuditLogger, buildAuditProvider(params), "kafka"))
    }
    val authProvider = buildAuthProvider(params)

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    KafkaDataStoreConfig(catalog, brokers, zookeepers, consumers, partitions, replication,
      producerConfig, consumerConfig, consumeFromBeginning, cacheExpiry, cacheCleanup, cacheConsistency,
      ticker, cqEngine, looseBBox, authProvider, audit, ns)
  }

  private def buildAuthProvider(params: java.util.Map[String, Serializable]): AuthorizationsProvider = {
    import KafkaDataStoreFactoryParams.Authorizations
    // get the auth params passed in as a comma-delimited string
    val auths = Authorizations.lookupOpt(params).map(_.split(",").filterNot(_.isEmpty)).getOrElse(Array.empty)
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
    ZkPath.lookupOpt(params)
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
        case NonFatal(_) => brokers
      }
    }
  }

  private def toProperties(param: String): Properties = {
    val props = new Properties
    props.load(new StringReader(param))
    props
  }

  // noinspection TypeAnnotation
  object KafkaDataStoreFactoryParams extends NamespaceParams {
    val Brokers          = new GeoMesaParam[String]("kafka.brokers", "Kafka brokers", required = true, deprecated = Seq("brokers"))
    val Zookeepers       = new GeoMesaParam[String]("kafka.zookeepers", "Kafka zookeepers", required = true, deprecated = Seq("zookeepers"))
    val ZkPath           = new GeoMesaParam[String]("kafka.zk.path", "Zookeeper discoverable path (namespace)", default = DefaultZkPath, deprecated = Seq("zkPath"))
    val ProducerConfig   = new GeoMesaParam[String]("kafka.producer.config", "Configuration options for kafka producer, in Java properties format. See http://kafka.apache.org/documentation.html#producerconfigs", metadata = Map(Parameter.IS_LARGE_TEXT -> java.lang.Boolean.TRUE), deprecated = Seq("producerConfig"))
    val ConsumerConfig   = new GeoMesaParam[String]("kafka.consumer.config", "Configuration options for kafka consumer, in Java properties format. See http://kafka.apache.org/documentation.html#newconsumerconfigs", metadata = Map(Parameter.IS_LARGE_TEXT -> java.lang.Boolean.TRUE), deprecated = Seq("consumerConfig"))
    val ConsumeEarliest  = new GeoMesaParam[java.lang.Boolean]("kafka.consumer.from-beginning", "Start reading from the beginning of the topic (vs ignore old messages)", default = false)
    val TopicPartitions  = new GeoMesaParam[Integer]("kafka.topic.partitions", "Number of partitions to use in kafka topics", default = 1, deprecated = Seq("partitions"))
    val TopicReplication = new GeoMesaParam[Integer]("kafka.topic.replication", "Replication factor to use in kafka topics", default = 1, deprecated = Seq("replication"))
    val ConsumerCount    = new GeoMesaParam[Integer]("kafka.consumer.count", "Number of kafka consumers used per feature type. Set to 0 to disable consuming (i.e. producer only)", default = 1)
    val CacheExpiry      = new GeoMesaParam[Duration]("kafka.cache.expiry", "Features will be expired after this delay", default = Duration.Inf)
    val CacheCleanup     = new GeoMesaParam[Duration]("kafka.cache.cleanup", "Run a thread at this interval to clean expired features from the cache (vs cleanup during reads and writes)", default = Duration("30s"))
    val CacheConsistency = new GeoMesaParam[Duration]("kafka.cache.consistency", "Check the feature cache for consistency at this interval")
    val CacheTicker      = new GeoMesaParam[Ticker]("kafka.cache.ticker", "Ticker to use for expiring/cleaning feature cache")
    val CqEngineCache    = new GeoMesaParam[java.lang.Boolean]("kafka.cache.cqengine", "Use CQEngine-based implementation of live feature cache", default = false, deprecated = Seq("useCQCache"))
    val LooseBBox        = GeoMesaDataStoreFactory.LooseBBoxParam
    val AuditQueries     = GeoMesaDataStoreFactory.AuditQueriesParam
    val Authorizations   = org.locationtech.geomesa.security.AuthsParam
  }
}
