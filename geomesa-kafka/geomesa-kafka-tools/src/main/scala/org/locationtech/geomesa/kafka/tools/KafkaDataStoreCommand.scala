/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import java.io.File

import org.apache.kafka.clients.producer.Producer
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
import org.locationtech.geomesa.tools.{DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

/**
  * Abstract class for commands that require a KafkaDataStore
  */
trait KafkaDataStoreCommand extends DataStoreCommand[KafkaDataStore] {

  override def params: KafkaDataStoreParams

  override def connection: Map[String, String] = {
    val readBack = Option(params.readBack).map(_.toString).getOrElse {
      if (params.fromBeginning) { "Inf" } else { null }
    }
    Map[String, String](
      KafkaDataStoreFactoryParams.Brokers.getName          -> params.brokers,
      KafkaDataStoreFactoryParams.Zookeepers.getName       -> params.zookeepers,
      KafkaDataStoreFactoryParams.ZkPath.getName           -> params.zkPath,
      KafkaDataStoreFactoryParams.ConsumerCount.getName    -> params.numConsumers.toString,
      KafkaDataStoreFactoryParams.TopicPartitions.getName  -> params.partitions.toString,
      KafkaDataStoreFactoryParams.TopicReplication.getName -> params.replication.toString,
      KafkaDataStoreFactoryParams.ConsumerReadBack.getName -> readBack
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
