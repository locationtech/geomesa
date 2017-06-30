/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import java.time.{Clock, Instant, ZoneId, ZoneOffset}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.junit.runner.RunWith
import org.locationtech.geomesa.lambda.LambdaTestRunnerTest.LambdaTest
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

/**
  * Base class for running all kafka/zk embedded tests
  */
@RunWith(classOf[JUnitRunner])
class LambdaTestRunnerTest extends Specification with LazyLogging {

  var kafka: EmbeddedKafka = _

  // add new tests here
  val specs: Seq[LambdaTest] = Seq(
    new LambdaDataStoreTest,
    new org.locationtech.geomesa.lambda.stream.kafka.KafkaStoreTest,
    new ZookeeperOffsetManagerTest
  )

  step {
    logger.info("Starting embedded kafka/zk")
    kafka = new EmbeddedKafka()
    logger.info("Started embedded kafka/zk")
    specs.foreach { s => s.brokers = kafka.brokers; s.zookeepers = kafka.zookeepers }
  }

  specs.foreach(link)

  step {
    logger.info("Stopping embedded kafka/zk")
    kafka.close()
    logger.info("Stopped embedded kafka/zk")
  }
}

object LambdaTestRunnerTest {

  val connector = new MockInstance("lambda").getConnector("root", new PasswordToken(""))

  trait LambdaTest extends Specification {

    val sftName = getClass.getSimpleName

    val connector = LambdaTestRunnerTest.connector

    val clock = new TestClock()
    val offsetManager = new InMemoryOffsetManager

    var brokers: String = _
    var zookeepers: String = _

    lazy val dsParams = Map(
      "accumulo.connector"  -> connector,
      // note the table needs to be different to prevent testing errors
      "accumulo.tableName"  -> sftName,
      "accumulo.zookeepers" -> zookeepers,
      "kafka.brokers"       -> brokers,
      "kafka.zookeepers"    -> zookeepers,
      "kafka.partitions"    -> 2,
      "expiry"              -> "100ms",
      "clock"               -> clock,
      "offsetManager"       -> offsetManager
    )
  }

  class TestClock extends Clock with java.io.Serializable {
    var tick: Long = 0L
    override def withZone(zone: ZoneId): Clock = null
    override def getZone: ZoneId = ZoneOffset.UTC
    override def instant(): Instant = Instant.ofEpochMilli(tick)
  }
}
