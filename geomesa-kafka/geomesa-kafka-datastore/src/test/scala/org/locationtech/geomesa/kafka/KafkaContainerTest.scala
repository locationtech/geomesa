/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

class KafkaContainerTest extends Specification with BeforeAfterAll with LazyLogging {

  protected val network = Network.newNetwork()

  // listener for other containers in the docker network
  val dockerNetworkBrokers = "kafka:19092"

  private val kafka =
    new KafkaContainer(KafkaContainerTest.KafkaImage)
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withListener(dockerNetworkBrokers)
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka")))

  lazy val brokers = kafka.getBootstrapServers

  override def beforeAll(): Unit = kafka.start()

  override def afterAll(): Unit = CloseWithLogging(kafka)
}

object KafkaContainerTest {
  val KafkaImage =
    DockerImageName.parse("apache/kafka-native")
        .withTag(sys.props.getOrElse("kafka.docker.tag", "3.9.1"))
}
