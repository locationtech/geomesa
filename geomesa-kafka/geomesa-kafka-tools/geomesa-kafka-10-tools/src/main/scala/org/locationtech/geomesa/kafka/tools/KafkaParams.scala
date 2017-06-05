/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper

/**
  * Shared Kafka-specific command line parameters
  */

trait KafkaConnectionParams {
  @Parameter(names = Array("-b", "--brokers"), description = "Brokers (host:port, comma separated)", required = true)
  var brokers: String = null

  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var zookeepers: String = null

  var zkPath: String
  val isProducer: Boolean
  var replication: String
  var partitions: String
}

trait OptionalZkPathParams extends KafkaConnectionParams {
  @Parameter(names = Array("-p", "--zkpath"), description = "Zookeeper path where feature schemas are saved")
  var zkPath: String = null
}

trait RequiredZkPathParams extends KafkaConnectionParams {
  @Parameter(names = Array("-p", "--zkpath"), description = "Zookeeper path where feature schemas are saved", required = true)
  override var zkPath: String = KafkaDataStoreHelper.DefaultZkPath
}

trait ProducerKDSConnectionParams extends RequiredZkPathParams {
  @Parameter(names = Array("--replication"), description = "Replication factor for Kafka topic")
  override var replication: String = null

  @Parameter(names = Array("--partitions"), description = "Number of partitions for the Kafka topic")
  override var partitions: String = null

  override val isProducer: Boolean = true
}

/**
  * For a producer KDS without exposing the replication/partitions settings to the user
  */
trait SimpleProducerKDSConnectionParams extends RequiredZkPathParams {
  override var replication: String = null
  override var partitions: String = null
  override val isProducer: Boolean = true
}

trait ConsumerKDSConnectionParams extends RequiredZkPathParams {
  override var replication: String = null
  override var partitions: String = null
  override val isProducer: Boolean = false
}
