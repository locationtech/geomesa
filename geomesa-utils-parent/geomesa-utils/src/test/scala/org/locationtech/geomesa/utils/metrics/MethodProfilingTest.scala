/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.metrics

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.concurrent.CopyOnWriteArrayList

@RunWith(classOf[JUnitRunner])
class MethodProfilingTest extends Specification {

  "MethodProfiling" should {
    "keep track of explicit timings" in {
      class Profiling extends MethodProfiling {
        val times = new CopyOnWriteArrayList[Long]()
        def slowMethod(): String = {
          Thread.sleep(10)
          "test"
        }
        def exec: String = {
          profile(time => times.add(time))(slowMethod())
        }
      }

      val profiling = new Profiling
      val result = profiling.exec
      result mustEqual "test"
      profiling.times.size() mustEqual 1
      profiling.times.get(0) must beGreaterThanOrEqualTo(10L)
    }
  }
}