/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08

import java.io.File
import java.nio.ByteBuffer

import kafka.consumer.{AssignmentContext, ConsumerConfig, PartitionAssignor}
import kafka.network.BlockingChannel
import kafka.utils.{Utils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient

object KafkaUtils08 {
  def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().buffer
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac)
  def createZkUtils(config: ConsumerConfig): ZkUtils08 =
    createZkUtils(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): ZkUtils08 = {
    // zkStringSerializer is required - otherwise topics won't be created correctly
    new ZkUtils08(new ZkClient(zkConnect, sessionTimeout, connectTimeout, ZKStringSerializer))
  }
  def rm(file: File): Unit = Utils.rm(file)
  def messageFormatClassName: String = classOf[KafkaGeoMessageFormatter].getName
  def brokerParam: String = "metadata.broker.list"
}
