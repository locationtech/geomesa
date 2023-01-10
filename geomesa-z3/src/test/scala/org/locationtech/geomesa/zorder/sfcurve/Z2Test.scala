/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
 * Copyright (c) 2015 Azavea.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.zorder.sfcurve

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z2Test extends Specification {

  "Z2 encoding" should {
    "interlaces bits" in {
      Z2(1,0).z mustEqual 1
      Z2(2,0).z mustEqual 4
      Z2(3,0).z mustEqual 5
      Z2(0,1).z mustEqual 2
      Z2(0,2).z mustEqual 8
      Z2(0,3).z mustEqual 10

    }

    "deinterlaces bits" in {
      Z2(23,13).decode  mustEqual (23, 13)
      Z2(Int.MaxValue, 0).decode mustEqual (Int.MaxValue, 0)
      Z2(0, Int.MaxValue).decode mustEqual (0, Int.MaxValue)
      Z2(Int.MaxValue, Int.MaxValue).decode mustEqual (Int.MaxValue, Int.MaxValue)
    }

    "unapply" in {
      val Z2(x,y) = Z2(3,5)
      x mustEqual 3
      y mustEqual 5
    }

    "replaces example in Tropf, Herzog paper" in {
      // Herzog example inverts x and y, with x getting higher sigfigs
      val rmin = Z2(5,3)
      val rmax = Z2(10,5)
      val p = Z2(4, 7)

      rmin.z mustEqual 27
      rmax.z mustEqual 102
      p.z mustEqual 58

      val (litmax, bigmin) = Z2.zdivide(p.z, rmin.z, rmax.z)

      litmax mustEqual 55
      bigmin mustEqual 74
    }

    "replicates the wikipedia example" in {
      val rmin = Z2(2,2)
      val rmax = Z2(3,6)
      val p = Z2(5, 1)

      rmin.z mustEqual 12
      rmax.z mustEqual 45
      p.z mustEqual 19

      val (litmax, bigmin) = Z2.zdivide(p.z, rmin.z, rmax.z)

      litmax mustEqual 15
      bigmin mustEqual 36
    }

    "support maxRanges" in {
      val ranges = Seq(
        ZRange(0L, 4611686018427387903L), // (sfc.index(-180, -90),      sfc.index(180, 90)),        // whole world
        ZRange(864691128455135232L, 4323455642275676160L), // (sfc.index(35, 65),         sfc.index(45, 75)),         // 10^2 degrees
        ZRange(4105065703422263800L, 4261005727442805282L), // (sfc.index(-90, -45),       sfc.index(90, 45)),         // half world
        ZRange(4069591195588206970L, 4261005727442805282L), // (sfc.index(35, 55),         sfc.index(45, 75)),         // 10x20 degrees
        ZRange(4105065703422263800L, 4202182393016524625L), // (sfc.index(35, 65),         sfc.index(37, 68)),         // 2x3 degrees
        ZRange(4105065703422263800L, 4203729178335734358L), // (sfc.index(35, 65),         sfc.index(40, 70)),         // 5^2 degrees
        ZRange(4097762467352558080L, 4097762468106131815L), // (sfc.index(39.999, 60.999), sfc.index(40.001, 61.001)), // small bounds
        ZRange(4117455696967246884L, 4117458209718964953L), // (sfc.index(51.0, 51.0),     sfc.index(51.1, 51.1)),     // small bounds
        ZRange(4117455696967246884L, 4117455697154258685L), // (sfc.index(51.0, 51.0),     sfc.index(51.001, 51.001)), // small bounds
        ZRange(4117455696967246884L, 4117455696967246886L) // (sfc.index(51.0, 51.0),     sfc.index(51.0000001, 51.0000001)) // 60 bits in common
      )

      foreach(ranges) { r =>
        val ret = Z2.zranges(Array(r), maxRanges = Some(1000))
        ret.length must beGreaterThanOrEqualTo(0)
        ret.length must beLessThanOrEqualTo(1000)
      }
    }
  }
}
