/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10

import kafka.admin.AdminUtils
import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.consumer.AssignmentContext
import kafka.network.BlockingChannel
import kafka.utils.ZKCheckedEphemeral
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat

case class ZkUtils10(zkUtils: kafka.utils.ZkUtils){
  def zkClient: ZkClient = zkUtils.zkClient
  def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel =
    ClientUtils.channelToOffsetManager(groupId, zkUtils, socketTimeoutMs, retryBackOffMs)
  def deleteTopic(topic: String): Unit = AdminUtils.deleteTopic(zkUtils, topic)
  def topicExists(topic: String): Boolean = AdminUtils.topicExists(zkUtils, topic)
  def getAllTopics: Seq[String] = zkUtils.getAllTopics()
  def createTopic(topic: String, partitions: Int, replication: Int) = AdminUtils.createTopic(zkUtils, topic, partitions, replication)
  def getLeaderForPartition(topic: String, partition: Int): Option[Int] = zkUtils.getLeaderForPartition(topic, partition)
  def createEphemeralPathExpectConflict(path: String, data: String): Unit = zkUtils.createEphemeralPathExpectConflict(path, data)
  def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                   data: String,
                                                   expectedCallerData: Any,
                                                   checker: (String, Any) => Boolean,
                                                   backoffTime: Int): Unit =
    new ZKCheckedEphemeral(path, data, zkUtils.zkConnection.getZookeeper, zkUtils.isSecure).create()
  def deletePath(path: String): Unit = zkUtils.deletePath(path)
  def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String =
    zkUtils.getConsumerPartitionOwnerPath(groupId, topic, partition)
  def getChildrenParentMayNotExist(path: String): Seq[String] = zkUtils.getChildrenParentMayNotExist(path)
  def getAllBrokersInCluster: Seq[Broker] = zkUtils.getAllBrokersInCluster
  def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext =
    new AssignmentContext(group, consumerId, excludeInternalTopics, zkUtils)
  def readData(path: String): (String, Stat) = zkUtils.readData(path)
  def fetchTopicMetadataFromZk(topic: String) = {
    val metadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils)
    KafkaTopicMetadata(metadata.topic, metadata.partitionMetadata.size)
  }
  def close(): Unit = zkUtils.close()
}

case class KafkaTopicMetadata(topicName: String, numberOfPartitions: Int)
