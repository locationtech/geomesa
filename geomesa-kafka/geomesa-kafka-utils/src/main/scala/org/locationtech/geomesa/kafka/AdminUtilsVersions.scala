/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils

/**
  * Reflection wrapper for AdminUtils methods between kafka versions 0.9 and 0.10
  */
object AdminUtilsVersions {

  private val methods = AdminUtils.getClass.getDeclaredMethods

  def createTopic(zkUtils: ZkUtils, topic: String, partitions: Int, replication: Int): Unit =
    _createTopic(zkUtils, topic, partitions, replication)

  private val _createTopic: (ZkUtils, String, Int, Int) => Unit = {
    val method = methods.find(_.getName == "createTopic").getOrElse {
      throw new NoSuchMethodException("Couldn't find AdminUtils.createTopic method")
    }
    val parameterTypes = method.getParameterTypes
    if (parameterTypes.length == 6) {
      val default5 = methods.find(_.getName == "createTopic$default$5").getOrElse {
        throw new NoSuchMethodException("Couldn't find AdminUtils.createTopic default parameter")
      }.invoke(AdminUtils)
      val default6 = methods.find(_.getName == "createTopic$default$6").getOrElse {
        throw new NoSuchMethodException("Couldn't find AdminUtils.createTopic default parameter")
      }.invoke(AdminUtils)
      (zk, topic, partitions, replication) =>
        method.invoke(AdminUtils, zk, topic, Int.box(partitions), Int.box(replication), default5, default6)
    } else if (parameterTypes.length == 5) {
      val default5 = methods.find(_.getName == "createTopic$default$5").getOrElse {
        throw new NoSuchMethodException("Couldn't find AdminUtils.createTopic default parameter")
      }.invoke(AdminUtils)
      (zk, topic, partitions, replication) =>
        method.invoke(AdminUtils, zk, topic, Int.box(partitions), Int.box(replication), default5)
    } else {
      throw new NoSuchMethodException(s"Couldn't find AdminUtils.createTopic method with correct parameters: $method")
    }
  }
}
