/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreParams
import org.locationtech.geomesa.lambda.data.LambdaDataStore.{LambdaConfig, PersistenceConfig}
import org.locationtech.geomesa.lambda.data.LambdaDataStoreFactory.Params
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import java.time.Clock
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

object LambdaDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {

  // note: this class is kept separate to avoid any runtime dependencies on Accumulo

  import scala.collection.JavaConverters._

  val BrokersParam =
    new GeoMesaParam[String](
      "lambda.kafka.brokers",
      "Kafka brokers",
      optional = false,
      deprecatedKeys = Seq("kafka.brokers"),
      supportsNiFiExpressions = true)

  val ZookeepersParam =
    new GeoMesaParam[String](
      "lambda.kafka.zookeepers",
      "Kafka zookeepers",
      deprecatedKeys = Seq("kafka.zookeepers"),
      supportsNiFiExpressions = true)

  val PartitionsParam =
    new GeoMesaParam[Integer](
      "lambda.kafka.partitions",
      "Number of partitions to use in kafka topics",
      default = Int.box(1),
      deprecatedKeys = Seq("kafka.partitions"),
      supportsNiFiExpressions = true)

  val ConsumersParam =
    new GeoMesaParam[Integer](
      "lambda.kafka.consumers",
      "Number of kafka consumers used per feature type",
      default = Int.box(1),
      deprecatedKeys = Seq("kafka.consumers"),
      supportsNiFiExpressions = true)

  val ProducerOptsParam =
    new GeoMesaParam[Properties](
      "lambda.kafka.producer.options",
      "Kafka producer configuration options, in Java properties format",
      largeText = true,
      deprecatedKeys = Seq("kafka.producer.options"))

  val ConsumerOptsParam =
    new GeoMesaParam[Properties](
      "lambda.kafka.consumer.options",
      "Kafka consumer configuration options, in Java properties format'",
      largeText = true,
      deprecatedKeys = Seq("kafka.consumer.options"))

  val ExpiryParam =
    new GeoMesaParam[Duration](
      "lambda.expiry",
      "Duration before features expire from transient store. Use 'Inf' " +
          "to prevent this store from participating in feature expiration",
      default = Duration("1h"),
      deprecatedKeys = Seq("expiry"),
      supportsNiFiExpressions = true)

  val PersistParam =
    new GeoMesaParam[java.lang.Boolean](
      "lambda.persist",
      "Whether this instance will persist expired features to long-term storage",
      default = java.lang.Boolean.TRUE,
      deprecatedKeys = Seq("persist"))

  val BatchSizeParam =
    new GeoMesaParam[Integer](
      "lambda.persist.batch.size",
      "Maximum number of features to persist in one run",
      default = 100,
    )

  // test params
  val ClockParam =
    new GeoMesaParam[Clock](
      "lambda.clock",
      "Clock instance to use for timing",
      deprecatedKeys = Seq("clock"))

  val OffsetManagerParam =
    new GeoMesaParam[OffsetManager](
      "lambda.offset-manager",
      "Offset manager instance to use",
      deprecatedKeys = Seq("lamdab.offset-manager", "offsetManager"))

  val ConsumerOffsetCommitInterval =
    new GeoMesaParam[FiniteDuration](
      "lambda.kafka.consumer.offset-commit-interval",
      "The frequency of committing offsets for the Kafka consumer",
      default = Duration(10, TimeUnit.SECONDS)
    )

  def parse(params: java.util.Map[String, _], namespace: String): LambdaConfig = {
    val brokers = BrokersParam.lookup(params)
    val persistence = if (!PersistParam.lookup(params).booleanValue) { None } else {
      ExpiryParam.lookupOpt(params).collect { case d: FiniteDuration =>
        PersistenceConfig(d, BatchSizeParam.lookup(params).intValue())
      }
    }

    val partitions = PartitionsParam.lookup(params).intValue
    val consumers = ConsumersParam.lookup(params).intValue

    val bootstrap = Map("bootstrap.servers" -> brokers)
    val consumerConfig = ConsumerOptsParam.lookupOpt(params).map(_.asScala.toMap).getOrElse(Map.empty) ++ bootstrap
    val producerConfig = ProducerOptsParam.lookupOpt(params).map(_.asScala.toMap).getOrElse(Map.empty) ++ bootstrap

    val zk = ZookeepersParam.lookupOpt(params).orElse(Params.Accumulo.ZookeepersParam.lookupOpt(params)).getOrElse {
      throw new IllegalArgumentException(s"Missing required key: ${ZookeepersParam.key}/${Params.Accumulo.ZookeepersParam.key}")
    }
    val zkNamespace = s"gm_lambda_$namespace"

    val offsetCommitInterval = ConsumerOffsetCommitInterval.lookup(params)

    LambdaConfig(zk, zkNamespace, producerConfig, consumerConfig, partitions, consumers, persistence, offsetCommitInterval)
  }
}
