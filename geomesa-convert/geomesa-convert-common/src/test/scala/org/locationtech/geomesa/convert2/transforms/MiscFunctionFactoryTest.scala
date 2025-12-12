/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MiscFunctionFactoryTest extends Specification {

  "MiscFunctionFactory" >> {
    "require" should {
      "return the passed-in argument, if it exists" >> {
        MiscFunctionFactory.require(Array("a")) mustEqual "a"
      }
      "throw an exception, if the passed-in argument is null" >> {
        MiscFunctionFactory.require(Array(null)) must throwAn[IllegalArgumentException]
      }
    }
  }

}
