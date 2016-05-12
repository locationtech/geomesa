/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka.common

import kafka.api.TopicMetadata
import kafka.consumer.AssignmentContext
import kafka.network.BlockingChannel
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat

trait ZkUtils {
  def zkClient: ZkClient
  def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel
  def deleteTopic(topic: String): Unit
  def topicExists(topic: String): Boolean
  def getAllTopics: Seq[String]
  def createTopic(topic: String, partitions: Int, replication: Int): Unit
  def getLeaderForPartition(topic: String, partition: Int): Option[Int]
  def createEphemeralPathExpectConflict(path: String, data: String): Unit
  def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                   data: String,
                                                   expectedCallerData: Any,
                                                   checker: (String, Any) => Boolean,
                                                   backoffTime: Int): Unit
  def deletePath(path: String): Unit
  def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String
  def getChildrenParentMayNotExist(path: String): Seq[String]
  def getAllBrokersInCluster: Seq[kafka.cluster.Broker]
  def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext
  def readData(path: String): (String, Stat)
  def fetchTopicMetadataFromZk(topic: String): TopicMetadata
  def close(): Unit
}
