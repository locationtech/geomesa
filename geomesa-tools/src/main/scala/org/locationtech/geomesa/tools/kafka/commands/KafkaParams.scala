/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander.Parameter

/**
  * Shared Kafka-specific command line parameters
  */

trait KafkaConnectionParams {
  @Parameter(names = Array("-b", "--brokers"), description = "Brokers (host[:port], comma separated)", required = true)
  var brokers: String = null

  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var zookeepers: String = null

  @Parameter(names = Array("-p", "--zkpath"), description = "Zookeeper path where SFT info is saved)", required = true)
  var zkPath: String = null

  val isProducer: Boolean
  var replication: String
  var partitions: String
}

trait ProducerKDSConnectionParams extends KafkaConnectionParams {
  @Parameter(names = Array("--replication"), description = "Replication factor for Kafka topic")
  override var replication: String = null

  @Parameter(names = Array("--partitions"), description = "Number of partitions for the Kafka topic")
  override var partitions: String = null

  override val isProducer: Boolean = true
}

trait ConsumerKDSConnectionParams extends KafkaConnectionParams {
  override val isProducer: Boolean = false
}
