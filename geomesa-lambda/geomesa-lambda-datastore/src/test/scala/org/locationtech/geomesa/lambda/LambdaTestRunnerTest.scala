/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.MiniCluster
import org.locationtech.geomesa.lambda.LambdaTestRunnerTest.LambdaTest
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.specs2.specification.core.{Env, Fragments}

import java.time.{Clock, Instant, ZoneId, ZoneOffset}

/**
  * Base class for running all kafka/zk embedded tests
  */
@RunWith(classOf[JUnitRunner])
class LambdaTestRunnerTest extends Specification with BeforeAfterAll with LazyLogging {

  var kafka: EmbeddedKafka = _

  // add new tests here
  val specs: Seq[LambdaTest] = Seq(
    new LambdaDataStoreTest,
    new org.locationtech.geomesa.lambda.stream.kafka.KafkaStoreTest,
    new ZookeeperOffsetManagerTest
  )

  override def beforeAll(): Unit = {
    logger.info("Starting embedded kafka/zk")
    kafka = new EmbeddedKafka()
    logger.info("Started embedded kafka/zk")
    specs.foreach { s => s.brokers = kafka.brokers; s.zookeepers = kafka.zookeepers }
  }

  override def map(fs: => Fragments, env: Env): Fragments =
    specs.foldLeft(super.map(fs, env))((fragments, spec) => fragments ^ spec.fragments(env))

  override def afterAll(): Unit = {
    logger.info("Stopping embedded kafka/zk")
    kafka.close()
    logger.info("Stopped embedded kafka/zk")
  }
}

object LambdaTestRunnerTest {

  trait LambdaTest extends Specification {

    val sftName = getClass.getSimpleName

    val clock = new TestClock()
    val offsetManager = new InMemoryOffsetManager

    var brokers: String = _
    var zookeepers: String = _

    lazy val dsParams = Map(
      "lambda.accumulo.instance.id" -> MiniCluster.cluster.getInstanceName,
      "lambda.accumulo.zookeepers"  -> MiniCluster.cluster.getZooKeepers,
      "lambda.accumulo.user"        -> MiniCluster.Users.root.name,
      "lambda.accumulo.password"    -> MiniCluster.Users.root.password,
      // note the table needs to be different to prevent testing errors
      "lambda.accumulo.catalog"     -> sftName,
      "lambda.kafka.brokers"        -> brokers,
      "lambda.kafka.zookeepers"     -> zookeepers,
      "lambda.kafka.partitions"     -> 2,
      "lambda.expiry"               -> "100ms",
      "lambda.clock"                -> clock,
      "lambda.offset-manager"       -> offsetManager
    )
  }

  class TestClock extends Clock with java.io.Serializable {
    var tick: Long = 0L
    override def withZone(zone: ZoneId): Clock = null
    override def getZone: ZoneId = ZoneOffset.UTC
    override def instant(): Instant = Instant.ofEpochMilli(tick)
  }
}
