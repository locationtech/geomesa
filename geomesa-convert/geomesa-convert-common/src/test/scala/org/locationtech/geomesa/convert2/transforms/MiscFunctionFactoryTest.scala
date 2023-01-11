/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MiscFunctionFactoryTest extends Specification {

  "MiscFunctionFactory" >> {
    "intToBoolean" should {
      "convert a 0 to false" >> {
        MiscFunctionFactory.intToBoolean(Array(Int.box(0))) mustEqual false
      }
      "convert a 1 to true" >> {
        MiscFunctionFactory.intToBoolean(Array(Int.box(1))) mustEqual true
      }
      "convert any int other than 0 to true" >> {
        MiscFunctionFactory.intToBoolean(Array(Int.box(1000))) mustEqual true
        MiscFunctionFactory.intToBoolean(Array(Int.box(-2))) mustEqual true
      }
      "return null for null input" >> {
        MiscFunctionFactory.intToBoolean(Array(null)) mustEqual null
      }
      "throw an error if faced with a non-int value" >> {
        MiscFunctionFactory.intToBoolean(Array(Double.box(0.55567))) must throwA[ClassCastException]
        MiscFunctionFactory.intToBoolean(Array("0")) must throwA[ClassCastException]
      }
    }

    "withDefault" should {
      "return the first argument, if it's not null" >> {
        MiscFunctionFactory.withDefault(Array("a", Int.box(1))) mustEqual "a"
      }
      "return the default if the first argument is null" >> {
        MiscFunctionFactory.withDefault(Array(null, Int.box(1))) mustEqual 1
      }
      "return the first non-null argument" >> {
        MiscFunctionFactory.withDefault(Array("a", "b", "c", "d")) mustEqual "a"
        MiscFunctionFactory.withDefault(Array(null, "b", "c", "d")) mustEqual "b"
        MiscFunctionFactory.withDefault(Array(null, null, "c", "d")) mustEqual "c"
        MiscFunctionFactory.withDefault(Array(null, null, null, "d")) mustEqual "d"
        MiscFunctionFactory.withDefault(Array(null, null, null, null)) must beNull
      }
    }

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
