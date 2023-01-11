/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.versions

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition

/**
  * Reflection wrapper for AdminClient/Admin methods
  */
object KafkaAdminVersions extends LazyLogging {

  private val methods = classOf[AdminClient].getMethods

  /**
   * Note: added in Kafka 2.4
   *
   * @param admin admin
   * @param groupId group id
   * @param partitions partitions to delete offsets for
   */
  def deleteConsumerGroupOffsets(admin: AdminClient, groupId: String, partitions: java.util.Set[TopicPartition]): Unit =
    _deleteConsumerGroupOffsets(admin, groupId, partitions)

  private val _deleteConsumerGroupOffsets: (AdminClient, String, java.util.Set[TopicPartition]) => Unit = {
    methods.find(m => m.getName == "deleteConsumerGroupOffsets" && m.getParameterCount == 2).map { m =>
      (admin: AdminClient, groupId: String, partitions: java.util.Set[TopicPartition]) =>
        tryInvocation(m.invoke(admin, groupId, partitions)): Unit
    }.getOrElse {
      logger.warn("This version of Kafka does not support deleteConsumerGroupOffsets")
      (_, _, _) => ()
    }
  }
}
