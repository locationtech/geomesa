/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import org.locationtech.geomesa.kafka.KafkaContainerTest
import org.locationtech.geomesa.kafka.confluent.ConfluentContainerTest.SchemaRegistryContainer
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

class ConfluentContainerTest extends KafkaContainerTest {

  private var container: SchemaRegistryContainer = _

  lazy val schemaRegistryUrl: String = s"http://${container.getHost}:${container.getFirstMappedPort}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    container =
      new SchemaRegistryContainer("kafka:9092")
          .withNetwork(network)
          .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("schema-registry")))
    container.start()
  }

  override def afterAll(): Unit = {
    try {
      if (container != null) {
        container.stop()
      }
    } finally {
      super.afterAll()
    }
  }
}

object ConfluentContainerTest {

  val SchemaRegistryImage =
    DockerImageName.parse("confluentinc/cp-schema-registry")
        .withTag(sys.props.getOrElse("confluent.docker.tag", "7.6.0"))

  class SchemaRegistryContainer(brokers: String, name: DockerImageName) extends GenericContainer[SchemaRegistryContainer](name) {

    def this(brokers: String) = this(brokers, SchemaRegistryImage)

    withExposedPorts(8081)
    withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", brokers)
    withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
  }
}
