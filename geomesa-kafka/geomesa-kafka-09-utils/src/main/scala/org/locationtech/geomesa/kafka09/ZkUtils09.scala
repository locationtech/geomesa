/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka09

import kafka.admin.AdminUtils
import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.consumer.AssignmentContext
import kafka.network.BlockingChannel
import kafka.utils.ZKCheckedEphemeral
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat
import org.locationtech.geomesa.kafka.common.ZkUtils

case class ZkUtils09(zkUtils: kafka.utils.ZkUtils) extends ZkUtils {
  override def zkClient: ZkClient = zkUtils.zkClient
  override def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel =
    ClientUtils.channelToOffsetManager(groupId, zkUtils, socketTimeoutMs, retryBackOffMs)
  override def deleteTopic(topic: String): Unit = AdminUtils.deleteTopic(zkUtils, topic)
  override def topicExists(topic: String): Boolean = AdminUtils.topicExists(zkUtils, topic)
  override def createTopic(topic: String, partitions: Int, replication: Int) = AdminUtils.createTopic(zkUtils, topic, partitions, replication)
  override def getLeaderForPartition(topic: String, partition: Int): Option[Int] = zkUtils.getLeaderForPartition(topic, partition)
  override def createEphemeralPathExpectConflict(path: String, data: String): Unit = zkUtils.createEphemeralPathExpectConflict(path, data)
  override def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                   data: String,
                                                   expectedCallerData: Any,
                                                   checker: (String, Any) => Boolean,
                                                   backoffTime: Int): Unit =
    new ZKCheckedEphemeral(path, data, zkUtils.zkConnection.getZookeeper, zkUtils.isSecure).create()
  override def deletePath(path: String): Unit = zkUtils.deletePath(path)
  override def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String =
    zkUtils.getConsumerPartitionOwnerPath(groupId, topic, partition)
  override def getChildrenParentMayNotExist(path: String): Seq[String] = zkUtils.getChildrenParentMayNotExist(path)
  override def getAllBrokersInCluster: Seq[Broker] = zkUtils.getAllBrokersInCluster
  override def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext =
    new AssignmentContext(group, consumerId, excludeInternalTopics, zkUtils)
  override def readData(path: String): (String, Stat) = zkUtils.readData(path)
  override def close(): Unit = zkUtils.close()
}
