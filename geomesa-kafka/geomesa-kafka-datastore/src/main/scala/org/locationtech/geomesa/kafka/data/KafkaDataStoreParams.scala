/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.util.Properties
import java.util.concurrent.ScheduledExecutorService

import com.github.benmanes.caffeine.cache.Ticker
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceParams
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.DefaultZkPath
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{ConvertedParam, DeprecatedParam, ReadWriteFlag}
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex

import scala.concurrent.duration.Duration

object KafkaDataStoreParams extends KafkaDataStoreParams

trait KafkaDataStoreParams extends NamespaceParams {
  // deprecated lookups
  private val DeprecatedProducer = ConvertedParam[java.lang.Integer, java.lang.Boolean]("isProducer", v => if (v) { 0 } else { 1 })
  private val DeprecatedOffset = ConvertedParam[Duration, String]("autoOffsetReset", v => if ("earliest".equalsIgnoreCase(v)) { Duration.Inf } else { null })
  private val DeprecatedEarliest = ConvertedParam[Duration, java.lang.Boolean]("kafka.consumer.from-beginning", v => if (v) { Duration.Inf } else { null })
  private val DeprecatedExpiry = ConvertedParam[Duration, java.lang.Long]("expirationPeriod", v => Duration(v, "ms"))
  private val DeprecatedConsistency = ConvertedParam[Duration, java.lang.Long]("consistencyCheck", v => Duration(v, "ms"))
  // noinspection TypeAnnotation
  private val DeprecatedCleanup = new DeprecatedParam[Duration] {
    override val key = "cleanUpCache"
    override def lookup(params: java.util.Map[String, _], required: Boolean): Duration = {
      val param = new GeoMesaParam[java.lang.Boolean](key, default = false)
      if (!param.lookup(params)) { Duration.Inf } else {
        Duration(new GeoMesaParam[String]("cleanUpCachePeriod", default = "10s").lookup(params))
      }
    }
  }

  val Brokers =
    new GeoMesaParam[String](
      "kafka.brokers",
      "Kafka brokers",
      optional = false,
      deprecatedKeys = Seq("brokers"),
      supportsNiFiExpressions = true
    )

  val Zookeepers =
    new GeoMesaParam[String](
      "kafka.zookeepers",
      "Kafka zookeepers",
      optional = false,
      deprecatedKeys = Seq("zookeepers"),
      supportsNiFiExpressions = true
    )

  val ZkPath =
    new GeoMesaParam[String](
      "kafka.zk.path",
      "Zookeeper discoverable path (namespace)",
      default = DefaultZkPath,
      deprecatedKeys = Seq("zkPath"),
      supportsNiFiExpressions = true
    )

  val ProducerConfig =
    new GeoMesaParam[Properties](
      "kafka.producer.config",
      "Configuration options for kafka producer, in Java properties format. " +
          "See http://kafka.apache.org/documentation.html#producerconfigs",
      largeText = true,
      deprecatedKeys = Seq("producerConfig"),
      readWrite = ReadWriteFlag.WriteOnly
    )

  val ConsumerConfig =
    new GeoMesaParam[Properties](
      "kafka.consumer.config",
      "Configuration options for kafka consumer, in Java properties format. " +
          "See http://kafka.apache.org/documentation.html#consumerconfigs",
      largeText = true,
      deprecatedKeys = Seq("consumerConfig"),
      readWrite = ReadWriteFlag.ReadOnly
    )

  val ClearOnStart =
    new GeoMesaParam[java.lang.Boolean](
      "kafka.producer.clear",
      "Send a 'clear' message on startup. " +
          "This will cause clients to ignore any data that was in the topic prior to startup",
      default = Boolean.box(false),
      readWrite = ReadWriteFlag.WriteOnly
    )

  val ConsumerReadBack =
    new GeoMesaParam[Duration](
      "kafka.consumer.read-back",
      "On start up, read messages that were written within this time frame (vs ignore old messages), " +
          "e.g. '1 hour'. Use 'Inf' to read all messages",
      deprecatedParams = Seq(DeprecatedOffset, DeprecatedEarliest),
      readWrite = ReadWriteFlag.ReadOnly
    )

  val TopicPartitions =
    new GeoMesaParam[Integer](
      "kafka.topic.partitions",
      "Number of partitions to use in new kafka topics",
      default = 1,
      deprecatedKeys = Seq("partitions"),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.WriteOnly
    )

  val TopicReplication =
    new GeoMesaParam[Integer](
      "kafka.topic.replication",
      "Replication factor to use in new kafka topics",
      default = 1,
      deprecatedKeys = Seq("replication"),
      readWrite = ReadWriteFlag.WriteOnly
    )

  val ConsumerCount =
    new GeoMesaParam[Integer](
      "kafka.consumer.count",
      "Number of kafka consumers used per feature type. Set to 0 to disable consuming (i.e. producer only)",
      default = 1,
      deprecatedParams = Seq(DeprecatedProducer),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val SerializationType =
    new GeoMesaParam[String](
      "kafka.serialization.type",
      "Type of serialization to use. Must be one of 'kryo' or 'avro'",
      default = "kryo",
      enumerations = Seq("kryo", "avro"),
      supportsNiFiExpressions = true
    )

  // TODO these should really be per-feature, not per datastore...

  val CacheExpiry =
    new GeoMesaParam[Duration](
      "kafka.cache.expiry",
      "Features will be expired after this delay",
      deprecatedParams = Seq(DeprecatedExpiry),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val DynamicCacheExpiry =
    new GeoMesaParam[String](
      "kafka.cache.expiry.dynamic",
      "Specify feature expiry dynamically based on CQL predicates. " +
          "Should be a TypeSafe configuration string with CQL predicates as keys and expiry durations as values. " +
          "Features that do not match any predicate will fall back to 'kafka.cache.expiry', if defined",
      largeText = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val EventTime =
    new GeoMesaParam[String](
      "kafka.cache.event-time",
      "Instead of message time, determine expiry based on feature data. " +
          "This can be an attribute name or a CQL expression, but it must evaluate to a date",
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val IndexResolutionX =
    new GeoMesaParam[Integer](
      "kafka.index.resolution.x",
      "Number of bins in the x-dimension of the spatial index",
      default = Int.box(360),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val IndexResolutionY =
    new GeoMesaParam[Integer](
      "kafka.index.resolution.y",
      "Number of bins in the y-dimension of the spatial index",
      default = Int.box(180),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val IndexTiers =
    new GeoMesaParam[String](
      "kafka.index.tiers",
      "Number and size (in degrees) and of tiers to use when indexing geometries with extents",
      default = SizeSeparatedBucketIndex.DefaultTiers.map { case (x, y) => s"$x:$y"}.mkString(","),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val CqEngineIndices =
    new GeoMesaParam[String](
      "kafka.index.cqengine",
      "Use CQEngine for indexing individual attributes. Specify as `name:type`, delimited by commas, where name " +
          "is an attribute and type is one of `default`, `navigable`, `radix`, `unique`, `hash` or `geometry`",
      deprecatedKeys = Seq("kafka.cache.cqengine.indices"),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val EventTimeOrdering =
    new GeoMesaParam[java.lang.Boolean](
      "kafka.cache.event-time.ordering",
      "Instead of message time, determine feature ordering based on event time data",
      default = Boolean.box(false),
      readWrite = ReadWriteFlag.ReadOnly
    )

  val LazyLoad =
    new GeoMesaParam[java.lang.Boolean](
      "kafka.consumer.start-on-demand",
      "Start consuming a topic only when that feature type is first requested. " +
          "This can reduce load if some layers are never queried",
      default = Boolean.box(true),
      readWrite = ReadWriteFlag.ReadOnly
    )

  val LazyFeatures =
    new GeoMesaParam[java.lang.Boolean](
      "kafka.serialization.lazy",
      "Use lazy deserialization of features. " +
          "This may improve processing load at the expense of slightly slower query times",
      default = Boolean.box(true),
      readWrite = ReadWriteFlag.ReadOnly
    )

  val LooseBBox: GeoMesaParam[java.lang.Boolean] = GeoMesaDataStoreFactory.LooseBBoxParam
  val AuditQueries: GeoMesaParam[java.lang.Boolean] = GeoMesaDataStoreFactory.AuditQueriesParam
  val Authorizations: GeoMesaParam[String] = org.locationtech.geomesa.security.AuthsParam

  val ExecutorTicker =
    new GeoMesaParam[(ScheduledExecutorService, Ticker)](
      "kafka.cache.executor",
      "Executor service and ticker to use for expiring features",
      readWrite = ReadWriteFlag.ReadOnly
    )

  @deprecated val CqEngineCache    = new GeoMesaParam[java.lang.Boolean]("kafka.cache.cqengine", "Use CQEngine-based implementation of live feature cache", default = Boolean.box(false), deprecatedKeys = Seq("useCQCache"))
  @deprecated val CacheCleanup     = new GeoMesaParam[Duration]("kafka.cache.cleanup", "Run a thread to clean expired features from the cache (vs cleanup during reads and writes)", default = Duration("30s"), deprecatedParams = Seq(DeprecatedCleanup))
  @deprecated val CacheConsistency = new GeoMesaParam[Duration]("kafka.cache.consistency", "Check the feature cache for consistency at this interval", deprecatedParams = Seq(DeprecatedConsistency))
  @deprecated val CacheTicker      = new GeoMesaParam[AnyRef]("kafka.cache.ticker", "Ticker to use for expiring/cleaning feature cache")
}
