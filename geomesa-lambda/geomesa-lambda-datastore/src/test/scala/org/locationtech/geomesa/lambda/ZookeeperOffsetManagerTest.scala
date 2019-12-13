/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.lambda.LambdaTestRunnerTest.LambdaTest
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.ZookeeperOffsetManager

class ZookeeperOffsetManagerTest extends LambdaTest with LazyLogging {

  import scala.concurrent.duration._

  sequential

  step {
    logger.info("ZookeeperOffsetManagerTest starting")
  }

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

  step {
    logger.info("ZookeeperOffsetManagerTest complete")
  }
}
