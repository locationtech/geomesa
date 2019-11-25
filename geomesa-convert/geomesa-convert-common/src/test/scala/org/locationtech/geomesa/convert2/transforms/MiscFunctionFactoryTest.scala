package org.locationtech.geomesa.convert2.transforms

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MiscFunctionFactoryTest extends Specification {

  "MiscFunctionFactory" >> {
    "intToBoolean" should {
      "convert a 0 to false" >> {
        MiscFunctionFactory.intToBoolean(0) mustEqual false
      }
      "convert a 1 to true" >> {
        MiscFunctionFactory.intToBoolean(1) mustEqual true
      }
      "convert any int other than 0 to true" >> {
        MiscFunctionFactory.intToBoolean(1000) mustEqual true
        MiscFunctionFactory.intToBoolean(-2) mustEqual true
      }
      "return null for null input" >> {
        MiscFunctionFactory.intToBoolean(null) mustEqual null
      }
      "throw an error if faced with a non-int value" >> {
        MiscFunctionFactory.intToBoolean(0.55567) must throwA[ClassCastException]
        MiscFunctionFactory.intToBoolean("0") must throwA[ClassCastException]
      }
    }

    "withDefault" should {
      "return the first argument, if it's not null" >> {
        MiscFunctionFactory.withDefault("a", 1) mustEqual "a"
      }
      "return the default if the first argument is null" >> {
        MiscFunctionFactory.withDefault(null, 1) mustEqual 1
      }
    }

    "require" should {
      "return the passed-in argument, if it exists" >> {
        MiscFunctionFactory.require("a") mustEqual "a"
      }
      "throw an exception, if the passed-in argument is null" >> {
        MiscFunctionFactory.require(null) must throwAn[IllegalArgumentException]
      }
    }
  }

}
