/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08

import kafka.admin.AdminUtils
import kafka.client.ClientUtils
import kafka.consumer.AssignmentContext
import kafka.network.BlockingChannel
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat

case class ZkUtils08(zkClient: ZkClient){
  def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel =
    ClientUtils.channelToOffsetManager(groupId, zkClient, socketTimeoutMs, retryBackOffMs)
  def deleteTopic(topic: String): Unit = AdminUtils.deleteTopic(zkClient, topic)
  def topicExists(topic: String): Boolean = AdminUtils.topicExists(zkClient, topic)
  def getAllTopics: Seq[String] = kafka.utils.ZkUtils.getAllTopics(zkClient)
  def createTopic(topic: String, partitions: Int, replication: Int) = AdminUtils.createTopic(zkClient, topic, partitions, replication)
  def getLeaderForPartition(topic: String, partition: Int): Option[Int] = kafka.utils.ZkUtils.getLeaderForPartition(zkClient, topic, partition)
  def createEphemeralPathExpectConflict(path: String, data: String): Unit = kafka.utils.ZkUtils.createEphemeralPathExpectConflict(zkClient, path, data)
  def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                            data: String,
                                                            expectedCallerData: Any,
                                                            checker: (String, Any) => Boolean,
                                                            backoffTime: Int): Unit =
    kafka.utils.ZkUtils.createEphemeralPathExpectConflictHandleZKBug(zkClient, path, data, expectedCallerData, checker, backoffTime)
  def deletePath(path: String) = kafka.utils.ZkUtils.deletePath(zkClient, path)
  def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String =
    kafka.utils.ZkUtils.getConsumerPartitionOwnerPath(groupId, topic, partition)
  def getChildrenParentMayNotExist(path: String): Seq[String] = kafka.utils.ZkUtils.getChildrenParentMayNotExist(zkClient, path)
  def getAllBrokersInCluster: Seq[kafka.cluster.Broker] = kafka.utils.ZkUtils.getAllBrokersInCluster(zkClient)
  def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext =
    new AssignmentContext(group, consumerId, excludeInternalTopics, zkClient)
  def readData(path: String): (String, Stat) = kafka.utils.ZkUtils.readData(zkClient, path)
  def fetchTopicMetadataFromZk(topic: String) = {
    val metadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
    KafkaTopicMetadata(metadata.topic, metadata.partitionsMetadata.size)
  }

  def close(): Unit = zkClient.close()
}

case class KafkaTopicMetadata(topicName: String, numberOfPartitions: Int)