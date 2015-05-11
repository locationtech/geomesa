/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kafka

import java.util.concurrent.atomic.AtomicInteger

import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.utils.TestUtils
import org.apache.curator.test.TestingServer
import org.specs2.mutable.Specification
import org.specs2.specification.{Fragments, Step}

trait SpecWithEmbeddedZookeeper extends Specification {

  private val zkServer = new TestingServer()
  val zookeeperConnect = zkServer.getConnectString
  private val brokerConf = TestUtils.createBrokerConfig(SpecWithEmbeddedZookeeper.nodeIds.getAndIncrement)
  brokerConf.setProperty("zookeeper.connect", zookeeperConnect) // override to use a unique zookeeper
  val brokerConnect = s"${brokerConf.getProperty("host.name")}:${brokerConf.getProperty("port")}"

  private val kafkaServer = new KafkaServerStartable(new KafkaConfig(brokerConf))
  kafkaServer.startup()

  override def map(fs: => Fragments) = fs ^ Step {
    try {
      kafkaServer.shutdown()
      zkServer.stop()
    } catch {
      case e: Exception =>
    }
  }
}

object SpecWithEmbeddedZookeeper {
  val nodeIds = new AtomicInteger(0)
}
