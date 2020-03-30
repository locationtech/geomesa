/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.kryo

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
