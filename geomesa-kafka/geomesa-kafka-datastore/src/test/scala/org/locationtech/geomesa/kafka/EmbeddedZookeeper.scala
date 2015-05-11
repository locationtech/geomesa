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

  val zkPort = TestUtils.choosePort()
  val zkConnect = "127.0.0.1:" + zkPort
  val zk = new EmbeddedZookeeper(zkPort)

  val brokerConf = TestUtils.createBrokerConfig(1)
  brokerConf.setProperty("zookeeper.connect", zkConnect) // override to use a unique zookeeper
  val server = TestUtils.createServer(new KafkaConfig(brokerConf))

  val host = brokerConf.getProperty("host.name")
  val port = brokerConf.getProperty("port").toInt
  val brokerConnect = s"$host:$port"

  def shutdown(): Unit =
    try {
      server.shutdown()
      zk.shutdown()
    } catch {
      case e: Exception =>
    }
}

class EmbeddedZookeeper(val port: Int) {
  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress("127.0.0.1", port), 1024)
  factory.startup(zookeeper)

  def shutdown(): Unit = {
    try { zookeeper.shutdown() } catch { case _: Throwable => }
    try { factory.shutdown() } catch { case _: Throwable => }
    Utils.rm(logDir)
    Utils.rm(snapshotDir)
  }

}