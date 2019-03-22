/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.awt.RenderingHints
import java.io.Serializable
import java.util.Properties
import java.util.concurrent.ScheduledExecutorService

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.DataStoreFactorySpi
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreInfo, NamespaceParams}
import org.locationtech.geomesa.index.metadata.MetadataStringSerializer
import org.locationtech.geomesa.kafka.data.KafkaDataStore._
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer.GeoMessageSerializerFactory
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, NoOpAuditProvider}
import org.locationtech.geomesa.utils.cache.Ticker
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{ConvertedParam, DeprecatedParam}
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex
import org.locationtech.geomesa.utils.zk.ZookeeperMetadata

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): KafkaDataStore =
    createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): KafkaDataStore = {
    val config = KafkaDataStoreFactory.buildConfig(params)
    val meta = new ZookeeperMetadata(s"${config.catalog}/$MetadataPath", config.zookeepers, MetadataStringSerializer)
    val ds = new KafkaDataStore(config, meta, new GeoMessageSerializerFactory())
    if (!LazyLoad.lookup(params)) {
      ds.startAllConsumers()
    }
    ds
  }

  override def getDisplayName: String = KafkaDataStoreFactory.DisplayName

  override def getDescription: String = KafkaDataStoreFactory.Description

  // note: we don't return producer configs, as they would not be used in geoserver
  override def getParametersInfo: Array[Param] = KafkaDataStoreFactory.ParameterInfo :+ NamespaceParam

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    KafkaDataStoreFactory.canProcess(params)

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object KafkaDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  import scala.collection.JavaConverters._

  val DefaultZkPath: String = "geomesa/ds/kafka"

  override val DisplayName = "Kafka (GeoMesa)"
  override val Description = "Apache Kafka\u2122 distributed log"

  // note: these are consumer-oriented and don't include producer configs
  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      KafkaDataStoreFactoryParams.Brokers,
      KafkaDataStoreFactoryParams.Zookeepers,
      KafkaDataStoreFactoryParams.ZkPath,
      KafkaDataStoreFactoryParams.ConsumerCount,
      KafkaDataStoreFactoryParams.ConsumerConfig,
      KafkaDataStoreFactoryParams.ConsumerReadBack,
      KafkaDataStoreFactoryParams.CacheExpiry,
      KafkaDataStoreFactoryParams.EventTime,
      KafkaDataStoreFactoryParams.SerializationType,
      KafkaDataStoreFactoryParams.CqEngineIndices,
      KafkaDataStoreFactoryParams.IndexResolutionX,
      KafkaDataStoreFactoryParams.IndexResolutionY,
      KafkaDataStoreFactoryParams.IndexTiers,
      KafkaDataStoreFactoryParams.EventTimeOrdering,
      KafkaDataStoreFactoryParams.LazyLoad,
      KafkaDataStoreFactoryParams.LazyFeatures,
      KafkaDataStoreFactoryParams.AuditQueries,
      KafkaDataStoreFactoryParams.LooseBBox,
      KafkaDataStoreFactoryParams.Authorizations
    )

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean = {
    KafkaDataStoreFactoryParams.Brokers.exists(params) &&
        KafkaDataStoreFactoryParams.Zookeepers.exists(params) &&
        !params.containsKey("kafka.schema.registry.url") // defer to confluent data store
  }

  def buildConfig(params: java.util.Map[String, Serializable]): KafkaDataStoreConfig = {
    import KafkaDataStoreFactoryParams._

    val catalog = createZkNamespace(params)
    val brokers = checkBrokerPorts(Brokers.lookup(params))
    val zookeepers = Zookeepers.lookup(params)

    val topics = TopicConfig(TopicPartitions.lookup(params).intValue(), TopicReplication.lookup(params).intValue())

    val consumers = {
      val count = ConsumerCount.lookup(params).intValue
      val props = ConsumerConfig.lookupOpt(params).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
      val readBack = ConsumerReadBack.lookupOpt(params)
      KafkaDataStore.ConsumerConfig(count, props, readBack)
    }

    val producers = {
      val props = ProducerConfig.lookupOpt(params).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
      KafkaDataStore.ProducerConfig(props)
    }
    val clearOnStart = ClearOnStart.lookup(params)

    val serialization = org.locationtech.geomesa.features.SerializationType.withName(SerializationType.lookup(params))

    val indices = {
      val cacheExpiry = CacheExpiry.lookupOpt(params).getOrElse(Duration.Inf)
      val cqEngine = {
        CqEngineIndices.lookupOpt(params) match {
          case Some(attributes) =>
            attributes.split(",").toSeq.map { attribute =>
              try {
                val Array(name, indexType) = attribute.split(":", 2)
                (name, CQIndexType.withName(indexType))
              } catch {
                case _: MatchError => throw new IllegalArgumentException(s"Invalid CQEngine index value: $attribute")
              }
            }

          case None =>
            // noinspection ScalaDeprecation
            if (!CqEngineCache.lookup(params).booleanValue()) { Seq.empty } else {
              logger.warn(s"Parameter '${CqEngineCache.key}' is deprecated, please use '${CqEngineIndices.key}' instead")
              Seq(KafkaDataStore.CqIndexFlag) // marker to trigger the cq engine index, will use config from the sft
            }

        }
      }
      val xBuckets = IndexResolutionX.lookup(params).intValue()
      val yBuckets = IndexResolutionY.lookup(params).intValue()
      val ssiTiers = parseSsiTiers(params)
      val lazyDeserialization = LazyFeatures.lookup(params).booleanValue()

      val eventTime = EventTime.lookupOpt(params).map { e =>
        EventTimeConfig(e, EventTimeOrdering.lookup(params).booleanValue())
      }

      val executor = ExecutorTicker.lookupOpt(params)

      IndexConfig(cacheExpiry, eventTime, xBuckets, yBuckets, ssiTiers, cqEngine, lazyDeserialization, executor)
    }

    val looseBBox = LooseBBox.lookup(params).booleanValue()

    val audit = if (!AuditQueries.lookup(params)) { None } else {
      Some((AuditLogger, buildAuditProvider(params), "kafka"))
    }
    val authProvider = buildAuthProvider(params)

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    // noinspection ScalaDeprecation
    Seq(CacheCleanup, CacheConsistency, CacheTicker).foreach { p =>
      if (params.containsKey(p.key)) {
        logger.warn(s"Parameter '${p.key}' is deprecated, and no longer has any effect")
      }
    }

    KafkaDataStoreConfig(catalog, brokers, zookeepers, consumers, producers, clearOnStart, topics, serialization,
      indices, looseBBox, authProvider, audit, ns)
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
    * Parse SSI tiers from parameters
    *
    * @param params params
    * @return
    */
  private [data] def parseSsiTiers(params: java.util.Map[String, Serializable]): Seq[(Double, Double)] = {
    def parse(tiers: String): Option[Seq[(Double, Double)]] = {
      try {
        val parsed = tiers.split(",").map { xy =>
          val Array(x, y) = xy.split(":")
          (x.toDouble, y.toDouble)
        }
        Some(parsed.toSeq.sorted)
      } catch {
        case NonFatal(e) => logger.warn(s"Ignoring invalid index tiers '$tiers': ${e.toString}"); None
      }
    }

    KafkaDataStoreFactoryParams.IndexTiers.lookupOpt(params).flatMap(parse).getOrElse(SizeSeparatedBucketIndex.DefaultTiers)
  }

  /**
    * Gets up a zk path parameter - trims, removes leading/trailing "/" if needed
    *
    * @param params data store params
    * @return
    */
  private [data] def createZkNamespace(params: java.util.Map[String, Serializable]): String = {
    KafkaDataStoreFactoryParams.ZkPath.lookupOpt(params)
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

  // noinspection TypeAnnotation
  object KafkaDataStoreFactoryParams extends NamespaceParams {
    // deprecated lookups
    private val DeprecatedProducer = ConvertedParam[java.lang.Integer, java.lang.Boolean]("isProducer", v => if (v) { 0 } else { 1 })
    private val DeprecatedOffset = ConvertedParam[Duration, String]("autoOffsetReset", v => if ("earliest".equalsIgnoreCase(v)) { Duration.Inf } else { null })
    private val DeprecatedEarliest = ConvertedParam[Duration, java.lang.Boolean]("kafka.consumer.from-beginning", v => if (v) { Duration.Inf } else { null })
    private val DeprecatedExpiry = ConvertedParam[Duration, java.lang.Long]("expirationPeriod", v => Duration(v, "ms"))
    private val DeprecatedConsistency = ConvertedParam[Duration, java.lang.Long]("consistencyCheck", v => Duration(v, "ms"))
    private val DeprecatedCleanup = new DeprecatedParam[Duration] {
      override val key = "cleanUpCache"
      override def lookup(params: java.util.Map[String, _ <: Serializable], required: Boolean): Duration = {
        val param = new GeoMesaParam[java.lang.Boolean](key, default = false)
        if (!param.lookup(params)) { Duration.Inf } else {
          Duration(new GeoMesaParam[String]("cleanUpCachePeriod", default = "10s").lookup(params))
        }
      }
    }

    val Brokers           = new GeoMesaParam[String]("kafka.brokers", "Kafka brokers", optional = false, deprecatedKeys = Seq("brokers"))
    val Zookeepers        = new GeoMesaParam[String]("kafka.zookeepers", "Kafka zookeepers", optional = false, deprecatedKeys = Seq("zookeepers"))
    val ZkPath            = new GeoMesaParam[String]("kafka.zk.path", "Zookeeper discoverable path (namespace)", default = DefaultZkPath, deprecatedKeys = Seq("zkPath"))
    val ProducerConfig    = new GeoMesaParam[Properties]("kafka.producer.config", "Configuration options for kafka producer, in Java properties format. See http://kafka.apache.org/documentation.html#producerconfigs", largeText = true, deprecatedKeys = Seq("producerConfig"))
    val ConsumerConfig    = new GeoMesaParam[Properties]("kafka.consumer.config", "Configuration options for kafka consumer, in Java properties format. See http://kafka.apache.org/documentation.html#newconsumerconfigs", largeText = true, deprecatedKeys = Seq("consumerConfig"))
    val ClearOnStart      = new GeoMesaParam[java.lang.Boolean]("kafka.producer.clear", "Send a 'clear' message on startup. This will cause clients to ignore any data that was in the topic prior to startup", default = Boolean.box(false))
    val ConsumerReadBack  = new GeoMesaParam[Duration]("kafka.consumer.read-back", "On start up, read messages that were written within this time frame (vs ignore old messages), e.g. '1 hour'. Use 'Inf' to read all messages", deprecatedParams = Seq(DeprecatedOffset, DeprecatedEarliest))
    val TopicPartitions   = new GeoMesaParam[Integer]("kafka.topic.partitions", "Number of partitions to use in new kafka topics", default = 1, deprecatedKeys = Seq("partitions"))
    val TopicReplication  = new GeoMesaParam[Integer]("kafka.topic.replication", "Replication factor to use in new kafka topics", default = 1, deprecatedKeys = Seq("replication"))
    val ConsumerCount     = new GeoMesaParam[Integer]("kafka.consumer.count", "Number of kafka consumers used per feature type. Set to 0 to disable consuming (i.e. producer only)", default = 1, deprecatedParams = Seq(DeprecatedProducer))
    val SerializationType = new GeoMesaParam[String]("kafka.serialization.type", "Type of serialization to use. Must be one of 'kryo' or 'avro'", default = "kryo", enumerations = Seq("kryo", "avro"))
    val ExecutorTicker    = new GeoMesaParam[(ScheduledExecutorService, Ticker)]("kafka.cache.executor", "Executor service and ticker to use for expiring features")
    val CacheExpiry       = new GeoMesaParam[Duration]("kafka.cache.expiry", "Features will be expired after this delay", deprecatedParams = Seq(DeprecatedExpiry))
    // TODO these should really be per-feature, not per datastore...
    val EventTime         = new GeoMesaParam[String]("kafka.cache.event-time", "Instead of message time, determine expiry based on feature data. This can be an attribute name or a CQL expression, but it must evaluate to a date")
    val IndexResolutionX  = new GeoMesaParam[Integer]("kafka.index.resolution.x", "Number of bins in the x-dimension of the spatial index", default = Int.box(360))
    val IndexResolutionY  = new GeoMesaParam[Integer]("kafka.index.resolution.y", "Number of bins in the y-dimension of the spatial index", default = Int.box(180))
    val IndexTiers        = new GeoMesaParam[String]("kafka.index.tiers", "Number and size (in degrees) and of tiers to use when indexing geometries with extents", default = SizeSeparatedBucketIndex.DefaultTiers.map { case (x, y) => s"$x:$y"}.mkString(","))
    val CqEngineIndices   = new GeoMesaParam[String]("kafka.index.cqengine", "Use CQEngine for indexing individual attributes. Specify as `name:type`, delimited by commas, where name is an attribute and type is one of `default`, `navigable`, `radix`, `unique`, `hash` or `geometry`", deprecatedKeys = Seq("kafka.cache.cqengine.indices"))
    val EventTimeOrdering = new GeoMesaParam[java.lang.Boolean]("kafka.cache.event-time.ordering", "Instead of message time, determine feature ordering based on event time data", default = Boolean.box(false))
    val LazyLoad          = new GeoMesaParam[java.lang.Boolean]("kafka.consumer.start-on-demand", "Start consuming a topic only when that feature type is first requested. This can reduce load if some layers are never queried", default = Boolean.box(true))
    val LazyFeatures      = new GeoMesaParam[java.lang.Boolean]("kafka.serialization.lazy", "Use lazy deserialization of features. This may improve processing load at the expense of slightly slower query times", default = Boolean.box(true))
    val LooseBBox         = GeoMesaDataStoreFactory.LooseBBoxParam
    val AuditQueries      = GeoMesaDataStoreFactory.AuditQueriesParam
    val Authorizations    = org.locationtech.geomesa.security.AuthsParam

    @deprecated val CqEngineCache    = new GeoMesaParam[java.lang.Boolean]("kafka.cache.cqengine", "Use CQEngine-based implementation of live feature cache", default = Boolean.box(false), deprecatedKeys = Seq("useCQCache"))
    @deprecated val CacheCleanup     = new GeoMesaParam[Duration]("kafka.cache.cleanup", "Run a thread to clean expired features from the cache (vs cleanup during reads and writes)", default = Duration("30s"), deprecatedParams = Seq(DeprecatedCleanup))
    @deprecated val CacheConsistency = new GeoMesaParam[Duration]("kafka.cache.consistency", "Check the feature cache for consistency at this interval", deprecatedParams = Seq(DeprecatedConsistency))
    @deprecated val CacheTicker      = new GeoMesaParam[AnyRef]("kafka.cache.ticker", "Ticker to use for expiring/cleaning feature cache")
  }
}
