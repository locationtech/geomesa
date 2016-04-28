/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka08

import java.io.File
import java.nio.ByteBuffer

import kafka.consumer.{AssignmentContext, PartitionAssignor}
import kafka.network.BlockingChannel
import kafka.utils.{Utils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.locationtech.geomesa.kafka.common.{ZkUtils, KafkaUtils}

class KafkaUtils08 extends KafkaUtils {
  override def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().buffer
  override def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac)
  override def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): ZkUtils = {
    // zkStringSerializer is required - otherwise topics won't be created correctly
    ZkUtils08(new ZkClient(zkConnect, sessionTimeout, connectTimeout, ZKStringSerializer))
  }
  override def rm(file: File): Unit = Utils.rm(file)
}
