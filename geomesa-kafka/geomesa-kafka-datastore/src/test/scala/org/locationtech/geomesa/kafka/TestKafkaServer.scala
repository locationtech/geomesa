/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.kafka

import java.net.InetSocketAddress

import kafka.server.KafkaConfig
import kafka.utils.{TestZKUtils, Utils, TestUtils}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

/** Creates a local Kafka instance for testing
  */
trait TestKafkaServer {

  val brokerConf = TestUtils.createBrokerConfig(1)

  val host = brokerConf.getProperty("host.name")
  val port = brokerConf.getProperty("port").toInt
  val broker = s"$host:$port"

  val zkConnect = TestZKUtils.zookeeperConnect
  val zk = new EmbeddedZookeeper(zkConnect)
  val server = TestUtils.createServer(new KafkaConfig(brokerConf))

  def shutdown(): Unit = {
    try {
      server.shutdown()
      zk.shutdown()
    } catch {
      case _: Throwable =>
    }
  }
}


class EmbeddedZookeeper(val connectString: String) {
  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val port = connectString.split(":")(1).toInt
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress("127.0.0.1", port), 1024)
  factory.startup(zookeeper)

  def shutdown() {
    try { zookeeper.shutdown() } catch { case _: Throwable => }
    try { factory.shutdown() } catch { case _: Throwable => }
    Utils.rm(logDir)
    Utils.rm(snapshotDir)
  }

}
