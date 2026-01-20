/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.locationtech.geomesa.kafka.KafkaWithZookeeperTest.ZookeeperContainer
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

// note: kafka is not actually using zookeeper (it's still in kraft mode), but we spin up a zookeeper for persisting geomesa schemas
class KafkaWithZookeeperTest extends KafkaContainerTest {

  private val zookeeper =
    new ZookeeperContainer(KafkaWithZookeeperTest.ZookeeperImage)
      .withExposedPorts(2181)

  lazy val zookeepers = s"${zookeeper.getHost}:${zookeeper.getMappedPort(2181)}"

  override def beforeAll(): Unit = {
    zookeeper.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    CloseWithLogging(zookeeper)
  }
}

object KafkaWithZookeeperTest {

  val ZookeeperImage =
    DockerImageName.parse("zookeeper")
      .withTag(sys.props.getOrElse("zookeeper.docker.tag", "3.9.2"))

  class ZookeeperContainer(image: DockerImageName) extends GenericContainer[ZookeeperContainer](image)
}
