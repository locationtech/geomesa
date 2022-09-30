/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.Producer
import org.locationtech.geomesa.kafka.data.{KafkaDataStore, KafkaDataStoreParams}
import org.locationtech.geomesa.tools.{DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import java.io.File
import java.nio.charset.StandardCharsets

/**
  * Abstract class for commands that require a KafkaDataStore
  */
trait KafkaDataStoreCommand extends DataStoreCommand[KafkaDataStore] {

  override def params: KafkaDataStoreParams

  override def connection: Map[String, String] = {
    val readBack = Option(params.readBack).map(_.toString).getOrElse {
      if (params.fromBeginning) { "Inf" } else { null }
    }
    val consumerProps =
      Option(params.consumerProperties).map(FileUtils.readFileToString(_, StandardCharsets.UTF_8)).orNull
    val producerProps =
      Option(params.producerProperties).map(FileUtils.readFileToString(_, StandardCharsets.UTF_8)).orNull

    Map[String, String](
      KafkaDataStoreParams.Brokers.getName          -> params.brokers,
      KafkaDataStoreParams.Zookeepers.getName       -> params.zookeepers,
      KafkaDataStoreParams.ZkPath.getName           -> params.zkPath,
      KafkaDataStoreParams.Catalog.getName          -> params.catalog,
      KafkaDataStoreParams.ConsumerCount.getName    -> params.numConsumers.toString,
      KafkaDataStoreParams.TopicPartitions.getName  -> params.partitions.toString,
      KafkaDataStoreParams.TopicReplication.getName -> params.replication.toString,
      KafkaDataStoreParams.ConsumerReadBack.getName -> readBack,
      KafkaDataStoreParams.ConsumerConfig.getName   -> consumerProps,
      KafkaDataStoreParams.ProducerConfig.getName   -> producerProps,
      KafkaDataStoreParams.CacheExpiry.getName      -> "0s",
      "kafka.schema.registry.url"                   -> params.schemaRegistryUrl
    ).filter(_._2 != null)
  }
}

object KafkaDataStoreCommand {

  trait KafkaDistributedCommand extends KafkaDataStoreCommand with DistributedCommand {

    abstract override def libjarsFiles: Seq[String] =
      Seq("org/locationtech/geomesa/kafka/tools/kafka-libjars.list") ++ super.libjarsFiles

    abstract override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_KAFKA_HOME", "lib"),
      () => ClassPathUtils.getJarsFromEnvironment("KAFKA_HOME"),
      () => ClassPathUtils.getJarsFromClasspath(classOf[KafkaDataStore]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[Producer[_, _]])
    ) ++ super.libjarsPaths
  }
}
