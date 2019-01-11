/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter

import scala.concurrent.duration.Duration

/**
  * Shared Kafka-specific command line parameters
  */

trait KafkaDataStoreParams {

  @Parameter(names = Array("-b", "--brokers"), description = "Brokers (host:port, comma separated)", required = true)
  var brokers: String = _

  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var zookeepers: String = _

  @Parameter(names = Array("-p", "--zkpath"), description = "Zookeeper path where feature schemas are saved")
  var zkPath: String = KafkaDataStoreFactory.DefaultZkPath

  def numConsumers: Int
  def replication: Int
  def partitions: Int
  def fromBeginning: Boolean
  def readBack: Duration
}

trait ProducerDataStoreParams extends KafkaDataStoreParams {

  @Parameter(names = Array("--replication"), description = "Replication factor for Kafka topic")
  var replication: Int = 1 // note: can't use override modifier since it's a var

  @Parameter(names = Array("--partitions"), description = "Number of partitions for the Kafka topic")
  var partitions: Int = 1 // note: can't use override modifier since it's a var

  override val numConsumers: Int = 0
  override val readBack: Duration = null
  override val fromBeginning: Boolean = false
}

trait ConsumerDataStoreParams extends KafkaDataStoreParams {

  @Parameter(names = Array("--num-consumers"), description = "Number of consumer threads used for reading from Kafka")
  var numConsumers: Int = 1 // note: can't use override modifier since it's a var

  @Parameter(names = Array("--from-beginning"), description = "Consume from the beginning or end of the topic")
  var fromBeginning: Boolean = false

  @Parameter(names = Array("--read-back"), description = "Consume messages written within this time frame, e.g. '1 hour'", converter = classOf[DurationConverter])
  var readBack: Duration = _

  override val replication: Int = 1
  override val partitions: Int = 1
}

trait StatusDataStoreParams extends KafkaDataStoreParams {
  override val numConsumers: Int = 0
  override val replication: Int = 1
  override val partitions: Int = 1
  override val readBack: Duration = null
  override val fromBeginning: Boolean = false
}
