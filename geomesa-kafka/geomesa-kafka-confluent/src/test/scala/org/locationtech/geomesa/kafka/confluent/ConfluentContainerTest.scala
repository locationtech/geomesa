/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.kafka.confluent.ConfluentContainerTest.SchemaRegistryContainer
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{GenericContainer, KafkaContainer, Network}
import org.testcontainers.utility.DockerImageName

class ConfluentContainerTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  private val network = Network.newNetwork()

  val kafka: KafkaContainer =
    new KafkaContainer(ConfluentContainerTest.KafkaImage)
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka")))

  private val container =
    new SchemaRegistryContainer("kafka:9092")
      .withNetwork(network)
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("schema-registry")))

  lazy val schemaRegistryUrl: String = s"http://${container.getHost}:${container.getFirstMappedPort}"

  override def beforeAll(): Unit = {
    kafka.start()
    container.start()
  }

  override def afterAll(): Unit = CloseWithLogging(Seq(container, kafka))
}

object ConfluentContainerTest {

  val SchemaRegistryImage =
    DockerImageName.parse("confluentinc/cp-schema-registry")
        .withTag(sys.props.getOrElse("confluent.docker.tag", "7.6.0"))

  val KafkaImage =
    DockerImageName.parse("confluentinc/cp-kafka")
      .withTag(sys.props.getOrElse("confluent.docker.tag", "7.6.0"))

  class SchemaRegistryContainer(brokers: String, name: DockerImageName) extends GenericContainer[SchemaRegistryContainer](name) {

    def this(brokers: String) = this(brokers, SchemaRegistryImage)

    withExposedPorts(8081)
    withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", brokers)
    withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
  }
}
