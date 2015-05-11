/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kafka

import java.net.InetSocketAddress

import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, Utils}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

trait HasEmbeddedZookeeper {
  val (brokerConnect, zkConnect) = EmbeddedZookeeper.connect()
  def shutdown(): Unit = EmbeddedZookeeper.shutdown()
}

object EmbeddedZookeeper {

  private var count = 0
  private var kafka: EmbeddedKafka = null

  def connect(): (String, String) = synchronized {
    count += 1
    if (count == 1) {
      kafka = new EmbeddedKafka
    }
    (kafka.brokerConnect, kafka.zkConnect)
  }

  def shutdown(): Unit = synchronized {
    count -= 1
    if (count == 0) {
      kafka.shutdown()
    }
  }
}

class EmbeddedKafka() {

  private val zkPort = TestUtils.choosePort()
  val zkConnect = "127.0.0.1:" + zkPort

  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress("127.0.0.1", zkPort), 1024)
  factory.startup(zookeeper)

  private val brokerConf = TestUtils.createBrokerConfig(1)
  brokerConf.setProperty("zookeeper.connect", zkConnect) // override to use a unique zookeeper
  val brokerConnect = s"${brokerConf.getProperty("host.name")}:${brokerConf.getProperty("port")}"
  private val server = TestUtils.createServer(new KafkaConfig(brokerConf))

  def shutdown(): Unit = {
    try { server.shutdown() } catch { case _: Throwable => }
    try { zookeeper.shutdown() } catch { case _: Throwable => }
    try { factory.shutdown() } catch { case _: Throwable => }
    Utils.rm(logDir)
    Utils.rm(snapshotDir)
  }

}