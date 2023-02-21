/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import org.junit.runner.RunWith
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.ZookeeperOffsetManager
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZookeeperOffsetManagerTest extends LambdaContainerTest {

  import scala.concurrent.duration._

  sequential

  "ZookeeperOffsetManager" should {
    "store and retrieve offsets" in {
      val manager = new ZookeeperOffsetManager(zookeepers, "ZookeeperOffsetManagerTest")
      forall(0 until 3)(i => manager.getOffset("foo", i) mustEqual -1L)
      (0 until 3).foreach(i => manager.setOffset("foo", i, i))
      forall(0 until 3)(i => manager.getOffset("foo", i) mustEqual i)
    }

    "trigger listeners" in {
      val manager = new ZookeeperOffsetManager(zookeepers, "ZookeeperOffsetManagerTest")
      val triggers = scala.collection.mutable.Map(0 -> 0, 1 -> 0, 2 -> 0)
      manager.addOffsetListener("bar", new OffsetListener() {
        override def offsetChanged(partition: Int, offset: Long): Unit = {
          triggers(partition) = offset.toInt
        }
      })

      manager.setOffset("bar", 0, 1)
      eventually(40, 100.millis)(triggers.toMap must beEqualTo(Map(0 -> 1, 1 -> 0, 2 -> 0)))
      manager.setOffset("bar", 0, 2)
      manager.setOffset("bar", 2, 1)
      eventually(40, 100.millis)(triggers.toMap must beEqualTo(Map(0 -> 2, 1 -> 0, 2 -> 1)))
    }
  }
}
