/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
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
