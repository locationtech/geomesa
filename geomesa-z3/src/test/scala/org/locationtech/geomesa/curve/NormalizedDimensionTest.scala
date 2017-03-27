/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NormalizedDimensionTest extends Specification {

  val prec: Long = math.pow(2, 31).toLong - 1
  val NormLat = NormalizedLat(prec)
  val NormLon = NormalizedLon(prec)

  "NormalizedDimension" should {

    "Round-trip normalize 0" >> {
      NormLat.normalize(NormLat.denormalize(0)) mustEqual 0
      NormLon.normalize(NormLon.denormalize(0)) mustEqual 0
    }

    "Round-trip normalize precision" >> {
      NormLat.normalize(NormLat.denormalize(prec)) mustEqual prec
      NormLon.normalize(NormLon.denormalize(prec)) mustEqual prec
    }
  }
}
