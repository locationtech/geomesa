/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.impl

import com.esotericsoftware.kryo.io.Output
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NonMutatingInputTest extends Specification with LazyLogging {

  "NonMutatingInput" should {
    "read and write strings" in {
      foreach(Seq("a", "foo", "nihao你好")) { s =>
        val out = new Output(128)
        out.writeString(s)
        val in = new NonMutatingInput()
        in.setBuffer(out.toBytes)
        in.readString() mustEqual s
      }
    }
  }
}
