/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.awt.RenderingHints
import java.io.{IOException, Serializable}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.DataStoreFactorySpi
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreInfo
import org.locationtech.geomesa.index.metadata.MetadataStringSerializer
import org.locationtech.geomesa.kafka.data.KafkaDataStore._
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer.GeoMessageSerializerFactory
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex
import org.locationtech.geomesa.utils.zk.ZookeeperMetadata

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams._

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
  override def getParametersInfo: Array[Param] =
    KafkaDataStoreFactory.ParameterInfo :+ NamespaceParam.asInstanceOf[Param]

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
  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      KafkaDataStoreParams.Brokers,
      KafkaDataStoreParams.Zookeepers,
      KafkaDataStoreParams.ZkPath,
      KafkaDataStoreParams.ConsumerCount,
      KafkaDataStoreParams.ConsumerConfig,
      KafkaDataStoreParams.ConsumerReadBack,
      KafkaDataStoreParams.CacheExpiry,
      KafkaDataStoreParams.DynamicCacheExpiry,
      KafkaDataStoreParams.EventTime,
      KafkaDataStoreParams.SerializationType,
      KafkaDataStoreParams.CqEngineIndices,
      KafkaDataStoreParams.IndexResolutionX,
      KafkaDataStoreParams.IndexResolutionY,
      KafkaDataStoreParams.IndexTiers,
      KafkaDataStoreParams.EventTimeOrdering,
      KafkaDataStoreParams.LazyLoad,
      KafkaDataStoreParams.LazyFeatures,
      KafkaDataStoreParams.AuditQueries,
      KafkaDataStoreParams.LooseBBox,
      KafkaDataStoreParams.Authorizations
    )

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean = {
    KafkaDataStoreParams.Brokers.exists(params) &&
        KafkaDataStoreParams.Zookeepers.exists(params) &&
        !params.containsKey("kafka.schema.registry.url") // defer to confluent data store
  }

  def buildConfig(params: java.util.Map[String, Serializable]): KafkaDataStoreConfig = {
    import KafkaDataStoreParams._

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
      val buckets = IndexResolution(IndexResolutionX.lookup(params), IndexResolutionY.lookup(params))
      val ssiTiers = parseSsiTiers(params)
      val lazyDeserialization = LazyFeatures.lookup(params).booleanValue()

      val expiry = {
        val simple = CacheExpiry.lookupOpt(params)
        val advanced = parseDynamicExpiry(params)
        val eventTime = EventTime.lookupOpt(params)
        val ordered = eventTime.isDefined && EventTimeOrdering.lookup(params).booleanValue()
        if (advanced.isEmpty) {
          simple.filter(_.isFinite()) match {
            case None => NeverExpireConfig
            case Some(e) if e.length == 0 => ImmediatelyExpireConfig
            case Some(e) => eventTime.map(EventTimeConfig(e, _, ordered)).getOrElse(IngestTimeConfig(e))
          }
        } else {
          // INCLUDE has already been validated to be the last element (if present) in parseDynamicExpiry
          val withDefault = if (advanced.last._1.equalsIgnoreCase("INCLUDE")) { advanced } else {
            advanced :+ ("INCLUDE" -> simple.getOrElse(Duration.Inf)) // add at the end
          }
          val configs = eventTime match {
            case None => withDefault.map { case (f, e) => f -> IngestTimeConfig(e) }
            case Some(ev) => withDefault.map { case (f, e) => f -> EventTimeConfig(e, ev, ordered) }
          }
          FilteredExpiryConfig(configs)
        }
      }

      val executor = ExecutorTicker.lookupOpt(params)

      IndexConfig(expiry, buckets, ssiTiers, cqEngine, lazyDeserialization, executor)
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
    import KafkaDataStoreParams.Authorizations
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

    KafkaDataStoreParams.IndexTiers.lookupOpt(params).flatMap(parse).getOrElse(SizeSeparatedBucketIndex.DefaultTiers)
  }

  /**
   * Parse the dynamic expiry param value into a seq of pairs
   *
   * @param params data store params
   * @return
   */
  private [data] def parseDynamicExpiry(params: java.util.Map[String, Serializable]): Seq[(String, Duration)] = {
    lazy val key = s"Invalid property for parameter '${KafkaDataStoreParams.DynamicCacheExpiry.key}'"
    val expiry = KafkaDataStoreParams.DynamicCacheExpiry.lookupOpt(params).toSeq.flatMap { value =>
      ConfigFactory.parseString(value).resolve().root().unwrapped().asScala.toSeq.map {
        case (filter, exp: String) =>
          // validate the filter, but leave it as a string so we can optimize it based on the sft later
          try { ECQL.toFilter(filter) } catch {
            case NonFatal(e) => throw new IOException(s"$key, expected a CQL filter but got: $filter", e)
          }
          val duration = try { Duration(exp) } catch {
            case NonFatal(e) => throw new IOException(s"$key, expected a duration for key '$filter' but got: $exp", e)
          }
          filter -> duration

        case (filter, exp) =>
          throw new IOException(s"$key, expected a JSON string for key '$filter' but got: $exp")
      }
    }
    if (expiry.dropRight(1).exists(_._1.equalsIgnoreCase("INCLUDE"))) {
      throw new IOException(s"$key, defined a filter after Filter.INCLUDE (which would never be invoked)")
    }
    expiry
  }

  /**
    * Gets up a zk path parameter - trims, removes leading/trailing "/" if needed
    *
    * @param params data store params
    * @return
    */
  private [data] def createZkNamespace(params: java.util.Map[String, Serializable]): String = {
    KafkaDataStoreParams.ZkPath.lookupOpt(params)
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

  @deprecated("Use KafkaDataStoreParams")
  object KafkaDataStoreFactoryParams extends KafkaDataStoreParams
}
