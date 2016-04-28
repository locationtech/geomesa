/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka09

import java.io.File
import java.nio.ByteBuffer

import kafka.consumer.{AssignmentContext, PartitionAssignor}
import kafka.network.BlockingChannel
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils
import org.locationtech.geomesa.kafka.common.{ZkUtils, KafkaUtils}

class KafkaUtils09 extends KafkaUtils {
  override def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().payload()
  override def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac).get(ac.consumerId).toMap
  override def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): ZkUtils =
    ZkUtils09(kafka.utils.ZkUtils(zkConnect, sessionTimeout, connectTimeout, JaasUtils.isZkSecurityEnabled))
  override def rm(file: File): Unit = Utils.delete(file)
}
