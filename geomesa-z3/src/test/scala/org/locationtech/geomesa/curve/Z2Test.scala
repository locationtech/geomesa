/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.locationtech.sfcurve.CoveredRange
import org.locationtech.sfcurve.zorder.{Z2, ZRange}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Z2Test extends Specification {

  val rand = new Random(-574)
  val maxInt = Z2SFC.lon.maxIndex
  def nextDim(): Int = rand.nextInt(maxInt)

  def padTo(s: String): String = (new String(Array.fill(62)('0')) + s).takeRight(62)

  "Z2" should {

    "apply and unapply" >> {
      val (x, y) = (nextDim(), nextDim())
      val z = Z2(x, y)
      z match { case Z2(zx, zy) =>
        zx mustEqual x
        zy mustEqual y
      }
    }

    "apply and unapply min values" >> {
      val (x, y) = (0, 0)
      val z = Z2(x, y)
      z match { case Z2(zx, zy) =>
        zx mustEqual x
        zy mustEqual y
      }
    }

    "apply and unapply max values" >> {
      foreach(Seq(Z2SFC, LegacyZ2SFC)) { sfc =>
        val (x, y) = (sfc.lon.maxIndex, sfc.lat.maxIndex)
        val z = Z2(x.toInt, y.toInt)
        z match { case Z2(zx, zy) =>
          zx mustEqual x
          zy mustEqual y
        }
      }
    }

    "fail for out-of-bounds values" >> {
      foreach(Seq(Z2SFC, LegacyZ2SFC)) { sfc =>
        foreach(Seq((-180.1, 0d), (0d, -90.1), (180.1, 0d), (0d, 90.1), (-181d, -91d), (181d, 91d))) {
          case (x, y) => sfc.index(x, y) must throwAn[IllegalArgumentException]
        }
      }
    }

    "split" >> {
      val splits = Seq(
        0x00000000ffffffL,
        0x00000000000000L,
        0x00000000000001L,
        0x000000000c0f02L,
        0x00000000000802L
      ) ++ (0 until 10).map(_ => nextDim().toLong)
      foreach(splits) { l =>
        val expected = padTo(new String(l.toBinaryString.toCharArray.flatMap(c => s"0$c")))
        padTo(Z2.split(l).toBinaryString) mustEqual expected
      }
    }

    "split and combine" >> {
      val z = nextDim()
      val split = Z2.split(z)
      val combined = Z2.combine(split)
      combined.toInt mustEqual z
    }

    "support bigmin" >> {
      val zmin = Z2(2, 2).z
      val zmax = Z2(3, 6).z
      val f = Z2(5, 1).z
      val (_, bigmin) = Z2.zdivide(f, zmin, zmax)
      Z2(bigmin).decode mustEqual((2, 4))
    }

    "support litmax" >> {
      val zmin = Z2(2, 2).z
      val zmax = Z2(3, 6).z
      val f = Z2(1, 7).z
      val (litmax, _) = Z2.zdivide(f, zmin, zmax)
      Z2(litmax).decode mustEqual((3, 5))
    }

    "calculate ranges" >> {
      val min = Z2(2, 2).z
      val max = Z2(3, 6).z
      val ranges = Z2.zranges(ZRange(min, max))
      ranges must haveLength(3)
      ranges must containTheSameElementsAs(
        Seq(
          CoveredRange(Z2(2, 2).z, Z2(3, 3).z),
          CoveredRange(Z2(2, 4).z, Z2(3, 5).z),
          CoveredRange(Z2(2, 6).z, Z2(3, 6).z)
        )
      )
    }

    "return non-empty ranges for a number of cases" >> {
      foreach(Seq(Z2SFC, LegacyZ2SFC)) { sfc =>
        val ranges = Seq(
          (sfc.index(-180, -90),      sfc.index(180, 90)),        // whole world
          (sfc.index(-90, -45),       sfc.index(90, 45)),         // half world
          (sfc.index(35, 65),         sfc.index(45, 75)),         // 10^2 degrees
          (sfc.index(35, 55),         sfc.index(45, 75)),         // 10x20 degrees
          (sfc.index(35, 65),         sfc.index(37, 68)),         // 2x3 degrees
          (sfc.index(35, 65),         sfc.index(40, 70)),         // 5^2 degrees
          (sfc.index(39.999, 60.999), sfc.index(40.001, 61.001)), // small bounds
          (sfc.index(51.0, 51.0),     sfc.index(51.1, 51.1)),     // small bounds
          (sfc.index(51.0, 51.0),     sfc.index(51.001, 51.001)), // small bounds
          (sfc.index(51.0, 51.0),     sfc.index(51.0000001, 51.0000001)) // 60 bits in common
        )

        def print(l: Z2, u: Z2, size: Int): Unit =
          println(s"${round(sfc.invert(l))} ${round(sfc.invert(u))}\t$size")
        def round(z: (Double, Double)): (Double, Double) =
          (math.round(z._1 * 1000.0) / 1000.0, math.round(z._2 * 1000.0) / 1000.0)

        foreach(ranges) { r =>
          val ret = Z2.zranges(ZRange(r._1, r._2))
          ret.length must beGreaterThan(0)
        }
      }
    }
  }
}
