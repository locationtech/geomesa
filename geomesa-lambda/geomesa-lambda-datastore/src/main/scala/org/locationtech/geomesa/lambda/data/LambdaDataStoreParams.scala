/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2b1d931578 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

<<<<<<< HEAD
=======
import java.time.Clock
import java.util.Properties

>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreParams
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.stream.{OffsetManager, ZookeeperOffsetManager}
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

<<<<<<< HEAD
import java.time.Clock
import java.util.Properties
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import scala.concurrent.duration.Duration

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
      optional = false,
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
      optional = false,
      default = Duration("1h"),
      deprecatedKeys = Seq("expiry"),
      supportsNiFiExpressions = true)

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
<<<<<<< HEAD
      "lambda.offset-manager",
      "Offset manager instance to use",
      deprecatedKeys = Seq("lamdab.offset-manager", "offsetManager"))
=======
      "lamdab.offset-manager",
      "Offset manager instance to use",
      deprecatedKeys = Seq("offsetManager"))
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))

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
