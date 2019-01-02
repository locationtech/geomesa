/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import org.junit.runner.RunWith
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BoundingBoxSamplerTest extends Specification {

  // Great pictures at http://www.bigdatamodeling.org/2013/01/intuitive-geohash.html

  "BoundingBoxSampler" should {
    "work with a cross" >> {
      val cross = new BoundingBoxSampler(new TwoGeoHashBoundingBox(GeoHash("d0"), GeoHash("g8")))
      testAndCheck(cross, 63,
        List("d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "db", "dc", "dd", "de",
          "df", "dg", "dh", "dj", "dk", "dm", "dn", "dp", "dq", "dr", "ds", "dt", "du", "dv", "dw",
          "dx", "dy", "dz", "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "ed", "ee",
          "eh", "ej", "ek", "em", "en", "ep", "eq", "er", "es", "et", "ew", "ex", "f0", "f2", "f8",
          "fb", "g0", "g2", "g8"))
    }

    "work with lat line" >> {
      val latRow = new BoundingBoxSampler(new TwoGeoHashBoundingBox(GeoHash("d0"), GeoHash("d8")))
      testAndCheck(latRow, 3, List("d0", "d2", "d8"))
    }

    "work with lon line" >> {
      val lonRow = new BoundingBoxSampler(new TwoGeoHashBoundingBox(GeoHash("d8"), GeoHash("dw")))
      testAndCheck(lonRow, 7, List("d8", "d9", "dd", "de", "ds", "dt", "dw"))
    }

    "work with bbox" >> {
      val box16 = new BoundingBoxSampler(new TwoGeoHashBoundingBox(GeoHash("dh"), GeoHash("dz")))
      testAndCheck(box16, 16,
        List("dh", "dj", "dk", "dm", "dn", "dp", "dq", "dr",
          "ds", "dt", "du", "dv", "dw", "dx", "dy", "dz"))
    }
  }

  def testAndCheck(bbs: BoundingBoxSampler, num: Int, list: List[String]): MatchResult[Any] = {
    val answer = for(i <- 1 to num) yield { bbs.next().hash }
    answer.toList.sorted mustEqual list
    try {
      bbs.next()
      ko("should have thrown NoSuchElementException")
    } catch {
      case nsee: NoSuchElementException => ok
    }
  }
}
