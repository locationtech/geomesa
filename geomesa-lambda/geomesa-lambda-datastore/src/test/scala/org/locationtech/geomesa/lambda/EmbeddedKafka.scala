/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.EmbeddedZookeeper
import org.apache.kafka.common.network.ListenerName
import org.locationtech.geomesa.utils.io.PathUtils

import java.io.Closeable

class EmbeddedKafka extends Closeable {

  private val zookeeper = new EmbeddedZookeeper()

  val zookeepers = s"127.0.0.1:${zookeeper.port}"

  private val logs = TestUtils.tempDir()
  private val server = {
    val config = TestUtils.createBrokerConfig(1, zookeepers)
    config.setProperty("offsets.topic.num.partitions", "1")
    config.setProperty("listeners", s"PLAINTEXT://127.0.0.1:${TestUtils.RandomPort}")
    config.setProperty("log.dirs", logs.getAbsolutePath)
    TestUtils.createServer(new KafkaConfig(config))
  }

  val brokers = s"127.0.0.1:${server.boundPort(ListenerName.normalised("PLAINTEXT"))}"

  override def close(): Unit = {
    try { server.shutdown() } catch { case _: Throwable => }
    try { zookeeper.shutdown() } catch { case _: Throwable => }
    PathUtils.deleteRecursively(logs.toPath)
  }
}