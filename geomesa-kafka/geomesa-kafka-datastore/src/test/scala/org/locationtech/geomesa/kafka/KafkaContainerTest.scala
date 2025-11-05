/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.kafka.KafkaContainerTest.ZookeeperContainer
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.lifecycle.Startables
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

  private val zookeeper =
    new ZookeeperContainer(KafkaContainerTest.ZookeeperImage)
      .withExposedPorts(2181)

  lazy val brokers = kafka.getBootstrapServers
  lazy val zookeepers = s"${zookeeper.getHost}:${zookeeper.getMappedPort(2181)}"

  override def beforeAll(): Unit = Startables.deepStart(kafka, zookeeper).get()

  override def afterAll(): Unit = CloseWithLogging(Seq(zookeeper, kafka))
}

object KafkaContainerTest {

  val KafkaImage =
    DockerImageName.parse("apache/kafka-native")
        .withTag(sys.props.getOrElse("kafka.docker.tag", "3.9.1"))

  val ZookeeperImage =
    DockerImageName.parse("zookeeper")
      .withTag(sys.props.getOrElse("zookeeper.docker.tag", "3.9.2"))

  class ZookeeperContainer(image: DockerImageName) extends GenericContainer[ZookeeperContainer](image)
}
