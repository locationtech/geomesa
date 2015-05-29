/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

import java.lang.IllegalArgumentException

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Z3RangeTest extends Specification {

  "Z3Range" should {

    val zmin = Z3(2, 2, 0)
    val zmax = Z3(3, 6, 0)
    val range = Z3Range(zmin, zmax)

    "require ordered min and max" >> {
      Z3Range(Z3(2, 2, 0), Z3(1, 4, 0)) // should be valid
      Z3Range(zmax, zmin) should throwA[IllegalArgumentException]
    }

    "support cut" >> {
      "for uncuttable ranges" >> {
        val range = Z3Range(zmin, zmin)
        Z3.cut(range, Z3(0, 0, 0), inRange = false) must beEmpty
      }
      "for out of range zs" >> {
        val zcut = Z3(5, 1, 0)
        Z3.cut(range, zcut, inRange = false) mustEqual
            List(Z3Range(zmin, Z3(3, 3, 0)), Z3Range(Z3(2, 4, 0), zmax))
      }
      "for in range zs" >> {
        val zcut = Z3(3, 4, 0)
        Z3.cut(range, zcut, inRange = true) mustEqual
            List(Z3Range(zmin, Z3(3, 4, 0)), Z3Range(Z3(2, 5, 0), zmax))
      }.pendingUntilFixed("should cut point be included in outputs?")
    }

    "support length" >> {
      range.length mustEqual 130
    }

    "support overlaps" >> {
      range.overlapsInUserSpace(range) must beTrue
      range.overlapsInUserSpace(Z3Range(Z3(3, 0, 0), Z3(3, 2, 0))) must beTrue
      range.overlapsInUserSpace(Z3Range(Z3(0, 0, 0), Z3(2, 2, 0))) must beTrue
      range.overlapsInUserSpace(Z3Range(Z3(1, 6, 0), Z3(4, 6, 0))) must beTrue
      range.overlapsInUserSpace(Z3Range(Z3(2, 0, 0), Z3(3, 1, 0))) must beFalse
      range.overlapsInUserSpace(Z3Range(Z3(4, 6, 0), Z3(6, 7, 0))) must beFalse
    }

    "support contains ranges" >> {
      range.containsInUserSpace(range) must beTrue
      range.containsInUserSpace(Z3Range(Z3(2, 2, 0), Z3(3, 3, 0))) must beTrue
      range.containsInUserSpace(Z3Range(Z3(3, 5, 0), Z3(3, 6, 0))) must beTrue
      range.containsInUserSpace(Z3Range(Z3(2, 2, 0), Z3(4, 3, 0))) must beFalse
      range.containsInUserSpace(Z3Range(Z3(2, 1, 0), Z3(3, 3, 0))) must beFalse
      range.containsInUserSpace(Z3Range(Z3(2, 1, 0), Z3(3, 7, 0))) must beFalse
    }
  }
}
