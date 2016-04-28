/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka.common

import java.io.File
import java.nio.ByteBuffer

import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, ConsumerThreadId, AssignmentContext, PartitionAssignor}
import kafka.network.BlockingChannel

trait KafkaUtils {
  def channelToPayload: (BlockingChannel) => ByteBuffer
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext): scala.collection.Map[TopicAndPartition, ConsumerThreadId]
  def createZkUtils(config: ConsumerConfig): ZkUtils =
    createZkUtils(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): ZkUtils
  def rm(file: File): Unit
}
