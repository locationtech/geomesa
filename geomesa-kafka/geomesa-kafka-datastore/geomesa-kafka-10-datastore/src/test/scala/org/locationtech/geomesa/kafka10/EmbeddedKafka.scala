/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10

import java.net.InetSocketAddress

import com.typesafe.scalalogging.LazyLogging
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

trait HasEmbeddedZookeeper {
  val zkConnect = EmbeddedZookeeper.connect()
  def shutdown(): Unit = EmbeddedZookeeper.shutdown()
}

trait HasEmbeddedKafka {
  val SYS_PROP_RUN_TESTS = "org.locationtech.geomesa.test-kafka"
  lazy val (brokerConnect, zkConnect) = EmbeddedKafka.connect()
  def shutdown(): Unit = EmbeddedKafka.shutdown()
}

trait EmbeddedServiceManager[S <: EmbeddedService[C], C] extends LazyLogging {

  private var count = 0
  private var service: S = null.asInstanceOf[S]

  def connect(): C = synchronized {
    count += 1
    if (count == 1) {
      logger.debug("Establishing connection")
      service = establishConnection()
      logger.debug("Connection established")
    }

    logger.debug("connect() complete - there are now {} tests using this service", count.asInstanceOf[AnyRef])
    service.connection
  }

  def shutdown(): Unit = synchronized {
    count -= 1
    if (count == 0) {
      logger.debug("Beginning shutdown")
      service.shutdown()
      logger.debug("Shutdown complete")
      service = null.asInstanceOf[S]
    }

    logger.info("shutdown() complete - there are now {} tests using this service", count.asInstanceOf[AnyRef])
  }

  protected def establishConnection(): S
}

object EmbeddedZookeeper extends EmbeddedServiceManager[EmbeddedZookeeper, String] {

  protected def establishConnection() = new EmbeddedZookeeper
}

object EmbeddedKafka extends EmbeddedServiceManager[EmbeddedKafka, (String, String)] {

  protected def establishConnection() = new EmbeddedKafka
}

trait EmbeddedService[C] extends AnyRef {

  def connection: C

  def shutdown(): Unit
}

class EmbeddedZookeeper extends EmbeddedService[String]{
  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress("127.0.0.1", new TestKafkaUtils10().choosePort), 1024)
  factory.startup(zookeeper)

  private val zkPort = zookeeper.getClientPort
  val zkConnect = "127.0.0.1:" + zkPort
  override def connection = zkConnect

  override def shutdown(): Unit = {
    try { zookeeper.shutdown() } catch { case _: Throwable => }
    try { factory.shutdown() } catch { case _: Throwable => }
    KafkaUtils10.rm(logDir)
    KafkaUtils10.rm(snapshotDir)
  }
}

class EmbeddedKafka extends EmbeddedService[(String, String)] {

  private lazy val zkConnect = EmbeddedZookeeper.connect()

  private val brokerConf = {
    val conf = new TestKafkaUtils10().createBrokerConfig(1, zkConnect)
    conf.setProperty("zookeeper.connect", zkConnect) // override to use a unique zookeeper
    conf.setProperty("host.name","localhost")
    conf.setProperty("port", "9092")
    conf
  }

  val brokerConnect = s"${brokerConf.getProperty("host.name")}:${brokerConf.getProperty("port")}"
  override def connection = (brokerConnect, zkConnect)


  private val server = TestUtils.createServer(new KafkaConfig(brokerConf))

  override def shutdown(): Unit = {
    try { server.shutdown() } catch { case _: Throwable => }

    EmbeddedZookeeper.shutdown()
  }

}