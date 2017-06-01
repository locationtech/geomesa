/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NormalizedDimensionTest extends Specification {

  val precision: Long = (1L << 31) - 1
  val NormLat = NormalizedLat(precision)
  val NormLon = NormalizedLon(precision)

  "NormalizedDimension" should {

    "Round-trip normalize minimum" >> {
      NormLat.normalize(NormLat.denormalize(0)) mustEqual 0
      NormLon.normalize(NormLon.denormalize(0)) mustEqual 0
    }

    "Round-trip normalize maximum" >> {
      NormLat.normalize(NormLat.denormalize(precision.toInt)) mustEqual precision
      NormLon.normalize(NormLon.denormalize(precision.toInt)) mustEqual precision
    }

    "Normalize mininimum" >> {
      NormLat.normalize(NormLat.min) mustEqual 0
      NormLon.normalize(NormLon.min) mustEqual 0
    }

    "Normalize maximum" >> {
      NormLat.normalize(NormLat.max) mustEqual precision
      NormLon.normalize(NormLon.max) mustEqual precision
    }

    "Denormalize 0 to min (special case)" >> {
      // this is to mirror use of Math.ceil in normalize
      NormLat.denormalize(0) mustEqual NormLat.min
      NormLon.denormalize(0) mustEqual NormLon.min
    }

    "Denormalize [1,precision] to bin middle" >> {
      // for any index/bin in range [1,precision] denormalize will return value in middle
      //   of range of coordinates that could result in index/bin from normalize call
      val latExtent = NormLat.max - NormLat.min
      val latWidth = latExtent / precision
      val epsilon = 1d / precision

      NormLat.denormalize(1) must beCloseTo(NormLat.min + latWidth / 2d, epsilon)
      NormLat.denormalize(precision.toInt) must beCloseTo(NormLat.max - latWidth / 2d, epsilon)

      val lonExtent = NormLon.max - NormLon.min
      val lonWidth = lonExtent / precision
      NormLon.denormalize(1) must beCloseTo(NormLon.min + lonWidth / 2d, epsilon)
      NormLon.denormalize(precision.toInt) must beCloseTo(NormLon.max - lonWidth / 2d, epsilon)
    }

  }
}
