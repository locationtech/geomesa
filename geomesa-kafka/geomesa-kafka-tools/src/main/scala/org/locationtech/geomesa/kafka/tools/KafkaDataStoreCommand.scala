/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import com.beust.jcommander.{IValueValidator, ParameterException}
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.Producer
import org.locationtech.geomesa.kafka.data.{KafkaDataStore, KafkaDataStoreParams}
import org.locationtech.geomesa.tools.{DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Locale

/**
  * Abstract class for commands that require a KafkaDataStore
  */
trait KafkaDataStoreCommand extends DataStoreCommand[KafkaDataStore] {

  override def params: KafkaDataStoreParams

  override def connection: Map[String, String] = {
    val readBack = Option(params.readBack).map(_.toString).getOrElse {
      if (params.fromBeginning) { "Inf" } else { null }
    }

    val genericProps = if (params.genericProperties == null) { null } else {
       FileUtils.readFileToString(params.genericProperties, StandardCharsets.UTF_8)
    }

    def mergeProps(f: File): String = {
      if (f == null) { genericProps } else {
        val p = FileUtils.readFileToString(f, StandardCharsets.UTF_8)
        if (genericProps == null) { p } else { s"$genericProps\n$p" } // note: later keys overwrite earlier ones
      }
    }

    val consumerProps = mergeProps(params.consumerProperties)
    val producerProps = mergeProps(params.producerProperties)

    Map[String, String](
      KafkaDataStoreParams.Brokers.getName           -> params.brokers,
      KafkaDataStoreParams.Zookeepers.getName        -> params.zookeepers,
      KafkaDataStoreParams.ZkPath.getName            -> params.zkPath,
      KafkaDataStoreParams.Catalog.getName           -> params.catalog,
      KafkaDataStoreParams.ConsumerCount.getName     -> params.numConsumers.toString,
      KafkaDataStoreParams.TopicPartitions.getName   -> params.partitions.toString,
      KafkaDataStoreParams.TopicReplication.getName  -> params.replication.toString,
      KafkaDataStoreParams.ConsumerReadBack.getName  -> readBack,
      KafkaDataStoreParams.ConsumerConfig.getName    -> consumerProps,
      KafkaDataStoreParams.ProducerConfig.getName    -> producerProps,
      KafkaDataStoreParams.CacheExpiry.getName       -> "0s",
      KafkaDataStoreParams.SerializationType.getName -> params.serialization,
      "kafka.schema.registry.url"                    -> params.schemaRegistryUrl
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

  class SerializationValidator extends IValueValidator[String] {
    import KafkaDataStoreParams.SerializationTypes.Types
    override def validate(name: String, value: String): Unit = {
      if (value != null && !Types.contains(value.toLowerCase(Locale.US))) {
        throw new ParameterException(s"Invalid serialization type. Valid types are ${Types.mkString(", ")}")
      }
    }
  }
}
