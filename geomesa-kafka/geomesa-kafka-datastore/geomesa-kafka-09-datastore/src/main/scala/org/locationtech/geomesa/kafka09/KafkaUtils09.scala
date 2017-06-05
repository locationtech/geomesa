/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka09

import java.io.File
import java.nio.ByteBuffer

import kafka.consumer.{AssignmentContext, PartitionAssignor, ConsumerConfig}
import kafka.network.BlockingChannel
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils

object KafkaUtils09 {
  def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().payload()
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac).get(ac.consumerId).toMap
  def createZkUtils(config: ConsumerConfig): ZkUtils09 =
    createZkUtils(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): ZkUtils09 =
    new ZkUtils09(kafka.utils.ZkUtils(zkConnect, sessionTimeout, connectTimeout, JaasUtils.isZkSecurityEnabled))
  def rm(file: File): Unit = Utils.delete(file)
  def messageFormatClassName: String = classOf[KafkaGeoMessageFormatter].getName
  def brokerParam: String = "metadata.broker.list"
}
