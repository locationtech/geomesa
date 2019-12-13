/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NormalizedDimensionTest extends Specification {

  val precision = 31
  val NormLat = NormalizedLat(precision)
  val NormLon = NormalizedLon(precision)
  val maxBin = (math.pow(2, precision) - 1).toInt // note: at 31 bits this is Int.MaxValue

  "NormalizedDimension" should {
    "Round-trip normalize minimum" >> {
      NormLat.normalize(NormLat.denormalize(0)) mustEqual 0
      NormLon.normalize(NormLon.denormalize(0)) mustEqual 0
    }

    "Round-trip normalize maximum" >> {
      NormLat.normalize(NormLat.denormalize(maxBin)) mustEqual maxBin
      NormLon.normalize(NormLon.denormalize(maxBin)) mustEqual maxBin
    }

    "Normalize mininimum" >> {
      NormLat.normalize(NormLat.min) mustEqual 0
      NormLon.normalize(NormLon.min) mustEqual 0
    }

    "Normalize maximum" >> {
      NormLat.normalize(NormLat.max) mustEqual maxBin
      NormLon.normalize(NormLon.max) mustEqual maxBin
    }

    "Denormalize [0,max - 1] to bin middle" >> {
      // for any index/bin denormalize will return value in middle of range of coordinates
      // that could result in index/bin from normalize call
      val latExtent = NormLat.max - NormLat.min
      val lonExtent = NormLon.max - NormLon.min
      val latWidth = latExtent / (maxBin.toLong + 1)
      val lonWidth = lonExtent / (maxBin.toLong + 1)

      NormLat.denormalize(0) mustEqual NormLat.min + latWidth / 2d
      NormLat.denormalize(maxBin) mustEqual NormLat.max - latWidth / 2d

      NormLon.denormalize(0) mustEqual NormLon.min + lonWidth / 2d
      NormLon.denormalize(maxBin) mustEqual NormLon.max - lonWidth / 2d
    }
  }
}
