/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}

import java.util.Collections
import scala.collection.JavaConverters._

object TruncateTopic extends LazyLogging {

  /**
   * Truncation of a Kafka topic
   *
   * @param admin admin
   * @param topic the topic name to truncate
   */
  def apply(admin: Admin, topic: String): Unit = {
    val deleteCleanupPolicy = hasDeleteCleanupPolicy(admin, topic)
    val latestRecords = getRecordsToDelete(admin, topic)
    try {
      if (!deleteCleanupPolicy) {
        logger.debug(s"Adding 'delete' cleanup policy to topic $topic so it can be truncated")
        alterDeleteCleanupPolicy(admin, topic, AlterConfigOp.OpType.APPEND)
      }
      logger.debug(s"Starting truncate of topic $topic")
      admin.deleteRecords(latestRecords.asJava).all().get()
      logger.debug(s"Completed truncate of topic $topic")
    } finally {
      if (!deleteCleanupPolicy) {
        logger.debug(s"Removing 'delete' cleanup policy to topic $topic")
        alterDeleteCleanupPolicy(admin, topic, AlterConfigOp.OpType.SUBTRACT)
      }
    }
  }

  /**
   * Get the latest offsets for a topic, by partition
   */
  private def getRecordsToDelete(admin: Admin, topic: String): Map[TopicPartition, RecordsToDelete] = {
    val topicInfo = admin.describeTopics(Collections.singleton(topic)).topicNameValues().get(topic).get()
    val partitions = topicInfo.partitions().asScala.map(info => new TopicPartition(topic, info.partition()) -> OffsetSpec.latest())
    val offsets = admin.listOffsets(partitions.toMap.asJava).all().get().asScala.toMap
    offsets.map { case (tp, info) => tp -> RecordsToDelete.beforeOffset(info.offset()) }
  }

  /**
   * Check if the topic has the 'delete' cleanup policy.
   */
  private def hasDeleteCleanupPolicy(admin: Admin, topicName: String): Boolean = {
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName)
    val configsResult = admin.describeConfigs(Collections.singleton(configResource))
    val config = configsResult.values().get(configResource).get()
    config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value().contains(TopicConfig.CLEANUP_POLICY_DELETE)
  }

  /**
   * Alter the 'delete' cleanup policy in the topic's 'cleanup.policy' config
   */
  private def alterDeleteCleanupPolicy(admin: Admin, topic: String, op: AlterConfigOp.OpType): Unit = {
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val alterConfigOp = new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE), op)
    admin.incrementalAlterConfigs(java.util.Map.of(configResource, Collections.singleton(alterConfigOp))).all().get()
  }
}
