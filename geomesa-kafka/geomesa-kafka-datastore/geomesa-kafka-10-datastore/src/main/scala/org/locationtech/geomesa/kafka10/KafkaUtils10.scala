/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10

import java.io.File
import java.nio.ByteBuffer

import kafka.consumer.{AssignmentContext, ConsumerConfig, PartitionAssignor}
import kafka.network.BlockingChannel
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils

object KafkaUtils10 {
  def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().payload()
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) =
    partitionAssignor.assign(ac).get(ac.consumerId).toMap
  def createZkUtils(config: ConsumerConfig): ZkUtils10 =
    createZkUtils(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): ZkUtils10 =
    new ZkUtils10(kafka.utils.ZkUtils(zkConnect, sessionTimeout, connectTimeout, JaasUtils.isZkSecurityEnabled))
  def rm(file: File): Unit = Utils.delete(file)
  def messageFormatClassName: String = classOf[KafkaGeoMessageFormatter].getName
  def brokerParam: String = "bootstrap.servers"
}
