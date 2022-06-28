/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import java.time.Clock
import java.util.Properties

import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreParams
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.stream.{OffsetManager, ZookeeperOffsetManager}
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import scala.concurrent.duration.Duration

object LambdaDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {

  // note: this class is kept separate to avoid any runtime dependencies on Accumulo

  import scala.collection.JavaConverters._

  val BrokersParam =
    new GeoMesaParam[String](
      "lambda.kafka.brokers",
      "Kafka brokers",
      optional = false,
      deprecatedKeys = Seq("kafka.brokers"))

  val ZookeepersParam =
    new GeoMesaParam[String](
      "lambda.kafka.zookeepers",
      "Kafka zookeepers",
      optional = false,
      deprecatedKeys = Seq("kafka.zookeepers"))

  val PartitionsParam =
    new GeoMesaParam[Integer](
      "lambda.kafka.partitions",
      "Number of partitions to use in kafka topics",
      default = Int.box(1),
      deprecatedKeys = Seq("kafka.partitions"))

  val ConsumersParam =
    new GeoMesaParam[Integer](
      "lambda.kafka.consumers",
      "Number of kafka consumers used per feature type",
      default = Int.box(1),
      deprecatedKeys = Seq("kafka.consumers"))

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
      optional = false,
      default = Duration("1h"),
      deprecatedKeys = Seq("expiry"))

  val PersistParam =
    new GeoMesaParam[java.lang.Boolean](
      "lambda.persist",
      "Whether to persist expired features to long-term storage",
      default = java.lang.Boolean.TRUE,
      deprecatedKeys = Seq("persist"))

  // test params
  val ClockParam =
    new GeoMesaParam[Clock](
      "lambda.clock",
      "Clock instance to use for timing",
      deprecatedKeys = Seq("clock"))

  val OffsetManagerParam =
    new GeoMesaParam[OffsetManager](
      "lamdab.offset-manager",
      "Offset manager instance to use",
      deprecatedKeys = Seq("offsetManager"))

  def parse(params: java.util.Map[String, _], namespace: String): LambdaConfig = {
    val brokers = BrokersParam.lookup(params)
    val expiry = ExpiryParam.lookup(params)

    val partitions = PartitionsParam.lookup(params).intValue
    val consumers = ConsumersParam.lookup(params).intValue
    val persist = PersistParam.lookup(params).booleanValue

    val bootstrap = Map("bootstrap.servers" -> brokers)
    val consumerConfig = ConsumerOptsParam.lookupOpt(params).map(_.asScala.toMap).getOrElse(Map.empty) ++ bootstrap
    val producerConfig = ProducerOptsParam.lookupOpt(params).map(_.asScala.toMap).getOrElse(Map.empty) ++ bootstrap

    val zk = ZookeepersParam.lookup(params)
    val zkNamespace = s"gm_lambda_$namespace"

    val offsetManager = OffsetManagerParam.lookupOpt(params).getOrElse(new ZookeeperOffsetManager(zk, zkNamespace))

    LambdaConfig(zk, zkNamespace, producerConfig, consumerConfig, partitions, consumers, expiry, persist, offsetManager)
  }
}
