/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.ZookeeperOffsetManager
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

@RunWith(classOf[JUnitRunner])
class ZookeeperOffsetManagerTest extends Specification with BeforeAfterAll with LazyLogging {

  import scala.concurrent.duration._

  private var container: GenericContainer[_] = _

  lazy val host = Option(container).map(_.getHost).getOrElse("localhost")
  lazy val port = Option(container).map(_.getFirstMappedPort).getOrElse(2181).toString
  lazy val zookeepers = s"$host:$port"

  override def beforeAll(): Unit = {
    val image =
      DockerImageName.parse("zookeeper")
          .withTag(sys.props.getOrElse("zookeeper.docker.tag", "3.6.4"))
    container = new GenericContainer(image)
    container.addExposedPort(2181)
    container.start()
    container.followOutput(new Slf4jLogConsumer(logger.underlying))
  }

  override def afterAll(): Unit = {
    if (container != null) {
      container.stop()
    }
  }

  "ZookeeperOffsetManager" should {
    "store and retrieve offsets" in {
      val manager = new ZookeeperOffsetManager(zookeepers, "ZookeeperOffsetManagerTest")
      val offsets = 0 until 3
      forall(offsets)(i => manager.getOffset("foo", i) mustEqual -1L)
      offsets.foreach(i => manager.setOffset("foo", i, i))
      forall(offsets)(i => manager.getOffset("foo", i) mustEqual i)
    }

    "trigger listeners" in {
      val manager = new ZookeeperOffsetManager(zookeepers, "ZookeeperOffsetManagerTest")
      val triggers = scala.collection.mutable.Map(0 -> 0, 1 -> 0, 2 -> 0)
      manager.addOffsetListener("bar", new OffsetListener() {
        override def offsetChanged(partition: Int, offset: Long): Unit =
          triggers(partition) = offset.toInt
      })

      manager.setOffset("bar", 0, 1)
      eventually(40, 100.millis)(triggers.toMap must beEqualTo(Map(0 -> 1, 1 -> 0, 2 -> 0)))
      manager.setOffset("bar", 0, 2)
      manager.setOffset("bar", 2, 1)
      eventually(40, 100.millis)(triggers.toMap must beEqualTo(Map(0 -> 2, 1 -> 0, 2 -> 1)))
    }
  }
}
