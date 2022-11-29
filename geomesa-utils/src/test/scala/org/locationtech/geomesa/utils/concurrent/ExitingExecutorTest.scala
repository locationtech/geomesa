/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor.NamedThreadFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.concurrent.{Executors, TimeUnit}

@RunWith(classOf[JUnitRunner])
class ExitingExecutorTest extends Specification {

  "NamedThreadFactory" should {
    "create new threads with specified name pattern" in {
      val pool = Executors.newFixedThreadPool(2, new NamedThreadFactory("geomesa-%d"))
      val name = pool.submit(() => {
        val first = Thread.currentThread().getName
        val second = pool.submit(() => Thread.currentThread().getName).get()
        Seq(first, second)
      }).get()
      name must equalTo(Seq("geomesa-0", "geomesa-1"))
      pool.shutdown()
      pool.awaitTermination(15, TimeUnit.SECONDS)
      pool.isTerminated must beTrue
    }
  }
}