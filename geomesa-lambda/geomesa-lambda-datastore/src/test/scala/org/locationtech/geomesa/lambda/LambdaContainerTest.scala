/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import com.typesafe.scalalogging.LazyLogging
import org.geomesa.testcontainers.AccumuloContainer
import org.locationtech.geomesa.lambda.LambdaContainerTest.TestClock
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.time.{Clock, Instant, ZoneId, ZoneOffset}

class LambdaContainerTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  private val network = Network.newNetwork()

  // listener for other containers in the docker network
  val dockerNetworkBrokers = "kafka:19092"

  val kafka =
    new KafkaContainer(LambdaContainerTest.KafkaImage)
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withListener(dockerNetworkBrokers)
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka")))

  val sftName = getClass.getSimpleName

  val clock = new TestClock()
  val offsetManager = new InMemoryOffsetManager

  lazy val dsParams = Map(
    "lambda.accumulo.instance.name" -> AccumuloContainer.getInstance().getInstanceName,
    "lambda.accumulo.zookeepers"    -> AccumuloContainer.getInstance().getZookeepers,
    "lambda.accumulo.user"          -> AccumuloContainer.getInstance().getUsername,
    "lambda.accumulo.password"      -> AccumuloContainer.getInstance().getPassword,
    // note the table needs to be different to prevent testing errors
    "lambda.accumulo.catalog"       -> sftName,
    "lambda.kafka.brokers"          -> kafka.getBootstrapServers,
    "lambda.kafka.partitions"       -> 2,
    "lambda.expiry"                 -> "100ms",
    "lambda.clock"                  -> clock,
    "lambda.offset-manager"         -> offsetManager
  )

  override def beforeAll(): Unit = kafka.start()

  override def afterAll(): Unit = CloseWithLogging(kafka)
}

object LambdaContainerTest {

  val KafkaImage =
    DockerImageName.parse("apache/kafka-native")
      .withTag(sys.props.getOrElse("kafka.docker.tag", "3.9.1"))

  class TestClock extends Clock with java.io.Serializable {

    var tick: Long = 0L

    override def withZone(zone: ZoneId): Clock = null
    override def getZone: ZoneId = ZoneOffset.UTC
    override def instant(): Instant = Instant.ofEpochMilli(tick)
  }
}
