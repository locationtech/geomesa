/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.curve.interop

import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpaceFillingCurvesTest extends Specification {

  val g: Short = 12

  "SpaceFillingCurves" should {
    "match Z2SFC index and hex encoding" >> {
      SpaceFillingCurves.z2Index(-77.0, 38.9) mustEqual Z2SFC.index(-77.0, 38.9)
      SpaceFillingCurves.z2Hex(-77.0, 38.9) mustEqual Z2SFC.hexEncode(Z2SFC.index(-77.0, 38.9))
    }

    "match XZ2SFC index and hex encoding" >> {
      val expected = XZ2SFC(g).index(-77.0, 38.0, -76.0, 39.0)
      SpaceFillingCurves.xz2Index(-77.0, 38.0, -76.0, 39.0, g) mustEqual expected
      SpaceFillingCurves.xz2Hex(-77.0, 38.0, -76.0, 39.0, g) mustEqual XZ2SFC(g).hexEncode(expected)
    }

    "clamp out-of-bounds coordinates to the WGS84 domain" >> {
      SpaceFillingCurves.z2Index(-180.0000001, 39.0) mustEqual SpaceFillingCurves.z2Index(-180.0, 39.0)
      SpaceFillingCurves.z2Index(116.0, 90.0000001) mustEqual SpaceFillingCurves.z2Index(116.0, 90.0)
      SpaceFillingCurves.xz2Index(-180.0000001, 39.0, -179.0, 90.0000001, g) mustEqual
          SpaceFillingCurves.xz2Index(-180.0, 39.0, -179.0, 90.0, g)
    }

    "produce Z2 hex ranges that cover indexed points in the envelope" >> {
      val ranges = SpaceFillingCurves.z2HexRanges(-80.0, 35.0, -70.0, 45.0, 2000)
      ranges.isEmpty must beFalse
      foreach(Seq((-75.0, 40.0), (-79.9, 35.1), (-70.1, 44.9))) { case (lon, lat) =>
        val hex = SpaceFillingCurves.z2Hex(lon, lat)
        ranges.stream().anyMatch(r => r.lower <= hex && hex <= r.upper) must beTrue
      }
    }

    "produce XZ2 hex ranges that cover indexed envelopes in the query" >> {
      val ranges = SpaceFillingCurves.xz2HexRanges(-80.0, 35.0, -70.0, 45.0, g, 2000)
      ranges.isEmpty must beFalse
      val hex = SpaceFillingCurves.xz2Hex(-75.0, 38.0, -74.0, 39.0, g)
      ranges.stream().anyMatch(r => r.lower <= hex && hex <= r.upper) must beTrue
    }

    "respect the maxRanges coarsening bound" >> {
      val fine = SpaceFillingCurves.z2HexRanges(-179.9, -89.9, 179.9, 89.9, 2000)
      val coarse = SpaceFillingCurves.z2HexRanges(-179.9, -89.9, 179.9, 89.9, 10)
      coarse.size must beLessThanOrEqualTo(fine.size)
      // the coarse cover must still contain everything the fine cover does
      val hex = SpaceFillingCurves.z2Hex(0.5, 0.5)
      coarse.stream().anyMatch(r => r.lower <= hex && hex <= r.upper) must beTrue
    }
  }
}
