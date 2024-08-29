/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.kafka.tools.KafkaDataStoreCommand.SerializationValidator
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter

import java.io.File
import scala.concurrent.duration.Duration

/**
  * Shared Kafka-specific command line parameters
  */

trait KafkaDataStoreParams {

  @Parameter(names = Array("-b", "--brokers"), description = "Brokers (host:port, comma separated)", required = true)
  var brokers: String = _

  @Parameter(names = Array("-p", "--zkpath"), description = "Zookeeper path where feature schemas are saved")
  var zkPath: String = _

  @Parameter(names = Array("-c", "--catalog"), description = "Kafka topic used for storing feature schemas")
  var catalog: String = _

  @Parameter(names = Array("--schema-registry"), description = "URL to a Confluent Schema Registry")
  var schemaRegistryUrl: String = _

  @Parameter(names = Array("--producer-config"), description = "Properties file used to configure the Kafka producer")
  var producerProperties: File = _

  @Parameter(names = Array("--consumer-config"), description = "Properties file used to configure the Kafka consumer")
  var consumerProperties: File = _

  @Parameter(names = Array("--config"), description = "Properties file used to configure the Kafka consumer/producer")
  var genericProperties: File = _

  def zookeepers: String
  def numConsumers: Int
  def replication: Int
  def partitions: Int
  def fromBeginning: Boolean
  def readBack: Duration
  def serialization: String
}

trait ProducerDataStoreParams extends KafkaDataStoreParams {

  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = _

  @Parameter(names = Array("--replication"), description = "Replication factor for Kafka topic")
  var replication: Int = 1 // note: can't use override modifier since it's a var

  @Parameter(names = Array("--partitions"), description = "Number of partitions for the Kafka topic")
  var partitions: Int = 1 // note: can't use override modifier since it's a var

  @Parameter(names = Array("--serialization"),
    description = "Serialization format to use, ones of 'kryo', 'avro', or 'avro-native'",
    validateValueWith = Array(classOf[SerializationValidator]))
  var serialization: String = _

  override val numConsumers: Int = 0
  override val readBack: Duration = null
  override val fromBeginning: Boolean = false
}

trait ConsumerDataStoreParams extends KafkaDataStoreParams {

  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = _

  @Parameter(names = Array("--num-consumers"), description = "Number of consumer threads used for reading from Kafka")
  var numConsumers: Int = 1 // note: can't use override modifier since it's a var

  @Parameter(names = Array("--from-beginning"), description = "Consume from the beginning or end of the topic")
  var fromBeginning: Boolean = false

  @Parameter(names = Array("--read-back"), description = "Consume messages written within this time frame, e.g. '1 hour'", converter = classOf[DurationConverter])
  var readBack: Duration = _

  override val serialization: String = null
  override val replication: Int = 1
  override val partitions: Int = 1
}

trait StatusDataStoreParams extends KafkaDataStoreParams {

  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = _

  override val serialization: String = null
  override val numConsumers: Int = 0
  override val replication: Int = 1
  override val partitions: Int = 1
  override val readBack: Duration = null
  override val fromBeginning: Boolean = false
}
