/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory

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
}

trait ProducerDataStoreParams extends KafkaDataStoreParams {
  @Parameter(names = Array("--replication"), description = "Replication factor for Kafka topic")
  var replication: Int = 1 // note: can't use override modifier since it's a var

  @Parameter(names = Array("--partitions"), description = "Number of partitions for the Kafka topic")
  var partitions: Int = 1 // note: can't use override modifier since it's a var

  override val numConsumers: Int = 0
  override val fromBeginning: Boolean = false
}

trait ConsumerDataStoreParams extends KafkaDataStoreParams {
  @Parameter(names = Array("--num-consumers"), description = "Number of consumer threads used for reading from Kafka")
  var numConsumers: Int = 1 // note: can't use override modifier since it's a var

  // TODO support from oldest+n, oldest+t, newest-n, newest-t, time=t, offset=o
  @Parameter(names = Array("--from-beginning"), description = "Consume from the beginning or end of the topic")
  var fromBeginning: Boolean = false

  override val replication: Int = 1
  override val partitions: Int = 1
}

trait StatusDataStoreParams extends KafkaDataStoreParams {
  override val numConsumers: Int = 0
  override val replication: Int = 1
  override val partitions: Int = 1
  override val fromBeginning: Boolean = false
}