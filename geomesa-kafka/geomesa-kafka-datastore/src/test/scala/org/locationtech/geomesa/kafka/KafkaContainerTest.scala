/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.{KafkaContainer, Network}
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

class KafkaContainerTest extends Specification with BeforeAfterAll with LazyLogging {

  private var container: KafkaContainer = _

  protected val network = Network.newNetwork()

  lazy val zookeepers = s"${container.getHost}:${container.getMappedPort(KafkaContainer.ZOOKEEPER_PORT)}"
  lazy val brokers = container.getBootstrapServers

  override def beforeAll(): Unit = {
    container =
      new KafkaContainer(KafkaContainerTest.KafkaImage)
          .withNetwork(network)
          .withNetworkAliases("kafka")
          .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka")))
    container.start()
  }

  override def afterAll(): Unit = {
    if (container != null) {
      container.stop()
    }
  }
}

object KafkaContainerTest {
  val KafkaImage =
    DockerImageName.parse("confluentinc/cp-kafka")
        .withTag(sys.props.getOrElse("confluent.docker.tag", "7.6.0"))
}
