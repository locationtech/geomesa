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
import org.locationtech.sfcurve.zorder.{Z3, ZRange}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Z3Test extends Specification {

  val rand = new Random(-574)
  val maxInt = Z3SFC(TimePeriod.Week).lon.maxIndex
  def nextDim(): Int = rand.nextInt(maxInt)

  def padTo(s: String): String = (new String(Array.fill(63)('0')) + s).takeRight(63)

  "Z3" should {

    "apply and unapply" >> {
      val (x, y, t) = (nextDim(), nextDim(), nextDim())
      val z = Z3(x, y, t)
      z match { case Z3(zx, zy, zt) =>
        zx mustEqual x
        zy mustEqual y
        zt mustEqual t
      }
    }

    "apply and unapply min values" >> {
      val (x, y, t) = (0, 0, 0)
      val z = Z3(x, y, t)
      z match { case Z3(zx, zy, zt) =>
        zx mustEqual x
        zy mustEqual y
        zt mustEqual t
      }
    }

    "apply and unapply max values" >> {
      foreach(Seq(Z3SFC(TimePeriod.Week), LegacyZ3SFC(TimePeriod.Week))) { sfc =>
        val (x, y, t) = (sfc.lon.maxIndex, sfc.lat.maxIndex, sfc.time.maxIndex)
        val z = Z3(x.toInt, y.toInt, t.toInt)
        z match { case Z3(zx, zy, zt) =>
          zx mustEqual x
          zy mustEqual y
          zt mustEqual t
        }
      }
    }

    "fail for out-of-bounds values" >> {
      foreach(Seq(Z3SFC(TimePeriod.Week), LegacyZ3SFC(TimePeriod.Week))) { sfc =>
        val toFail = Seq(
          (-180.1, 0d, 0L),
          (180.1, 0d, 0L),
          (0d, -90.1, 0L),
          (0d, 90.1, 0L),
          (0d, 0d, sfc.time.min.toLong - 1),
          (0d, 0d, sfc.time.max.toLong + 1),
          (-181d, -91d, sfc.time.min.toLong - 1),
          (181d, 91d, sfc.time.max.toLong + 1)
        )
        foreach(toFail) { case (x, y, t) => sfc.index(x, y, t) must throwAn[IllegalArgumentException] }
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
      splits.foreach { l =>
        val expected = padTo(new String(l.toBinaryString.toCharArray.flatMap(c => s"00$c")))
        padTo(Z3.split(l).toBinaryString) mustEqual expected
      }
      success
    }

    "split and combine" >> {
      val z = nextDim()
      val split = Z3.split(z)
      val combined = Z3.combine(split)
      combined.toInt mustEqual z
    }

    "support mid" >> {
      val (x, y, z)    = (0, 0, 0)
      val (x2, y2, z2) = (2, 2, 2)
      Z3(x, y, z).mid(Z3(x2, y2, z2)) match {
        case Z3(midx, midy, midz) =>
          midx mustEqual 1
          midy mustEqual 1
          midz mustEqual 1
      }
    }

    "support bigmin" >> {
      val zmin = Z3(2, 2, 0).z
      val zmax = Z3(3, 6, 0).z
      val f = Z3(5, 1, 0).z
      val (_, bigmin) = Z3.zdivide(f, zmin, zmax)
      Z3(bigmin).decode mustEqual((2, 4, 0))
    }

    "support litmax" >> {
      val zmin = Z3(2, 2, 0).z
      val zmax = Z3(3, 6, 0).z
      val f = Z3(1, 7, 0).z
      val (litmax, _) = Z3.zdivide(f, zmin, zmax)
      Z3(litmax).decode mustEqual((3, 5, 0))
    }

    "support in range" >> {
      val (x, y, z) = (nextDim(), nextDim(), nextDim())
      val z3 = Z3(x, y , z)
      val lessx  = Z3(x - 1, y, z)
      val lessx2 = Z3(x - 2, y, z)
      val lessy  = Z3(x, y - 1, z)
      val lessy2 = Z3(x, y - 2, z)
      val lessz  = Z3(x, y, z - 1)
      val lessz2 = Z3(x, y, z - 2)
      val less1  = Z3(x - 1, y - 1, z - 1)
      val less2  = Z3(x - 2, y - 2, z - 2)
      val morex  = Z3(x + 1, y, z)
      val morex2 = Z3(x + 2, y, z)
      val morey  = Z3(x, y + 1, z)
      val morez  = Z3(x, y, z + 1)
      val more1  = Z3(x + 1, y + 1, z + 1)

      z3.inRange(lessx, morex) must beTrue
      z3.inRange(lessx, morey) must beTrue
      z3.inRange(lessx, morez) must beTrue
      z3.inRange(lessx, more1) must beTrue

      z3.inRange(lessy, morex) must beTrue
      z3.inRange(lessy, morey) must beTrue
      z3.inRange(lessy, morez) must beTrue
      z3.inRange(lessy, more1) must beTrue

      z3.inRange(lessz, morex) must beTrue
      z3.inRange(lessz, morey) must beTrue
      z3.inRange(lessz, morez) must beTrue
      z3.inRange(lessz, more1) must beTrue

      z3.inRange(less1, more1) must beTrue

      z3.inRange(more1, less1) must beFalse
      z3.inRange(morex, morex2) must beFalse
      z3.inRange(lessx2, lessx) must beFalse
      z3.inRange(lessy2, lessy) must beFalse
      z3.inRange(lessz2, lessx) must beFalse
      z3.inRange(less2, less1) must beFalse
      z3.inRange(less2, more1) must beTrue
    }

    "calculate ranges" >> {
      val min = Z3(2, 2, 0).z
      val max = Z3(3, 6, 0).z
      val ranges = Z3.zranges(ZRange(min, max))
      ranges must haveLength(3)
      ranges must containTheSameElementsAs(
        Seq(
          CoveredRange(Z3(2, 2, 0).z, Z3(3, 3, 0).z),
          CoveredRange(Z3(2, 4, 0).z, Z3(3, 5, 0).z),
          CoveredRange(Z3(2, 6, 0).z, Z3(3, 6, 0).z))
      )
    }

    "return non-empty ranges for a number of cases" >> {
      foreach(Seq(Z3SFC(TimePeriod.Week), LegacyZ3SFC(TimePeriod.Week))) { sfc =>
        val week = sfc.time.max.toLong
        val day = sfc.time.max.toLong / 7
        val hour = sfc.time.max.toLong / 168

        val ranges = Seq(
          (sfc.index(-180, -90, 0), sfc.index(180, 90, week)), // whole world, full week
          (sfc.index(-180, -90, day), sfc.index(180, 90, day * 2)), // whole world, 1 day
          (sfc.index(-180, -90, hour * 10), sfc.index(180, 90, hour * 11)), // whole world, 1 hour
          (sfc.index(-180, -90, hour * 10), sfc.index(180, 90, hour * 64)), // whole world, 54 hours
          (sfc.index(-180, -90, day * 2), sfc.index(180, 90, week)), // whole world, 5 day
          (sfc.index(-90, -45, sfc.time.max.toLong / 4), sfc.index(90, 45, 3 * sfc.time.max.toLong / 4)), // half world, half week
          (sfc.index(35, 65, 0), sfc.index(45, 75, day)), // 10^2 degrees, 1 day
          (sfc.index(35, 55, 0), sfc.index(45, 65, week)), // 10^2 degrees, full week
          (sfc.index(35, 55, day), sfc.index(45, 75, day * 2)), // 10x20 degrees, 1 day
          (sfc.index(35, 55, day + hour * 6), sfc.index(45, 75, day * 2)), // 10x20 degrees, 18 hours
          (sfc.index(35, 65, day + hour), sfc.index(45, 75, day * 6)), // 10^2 degrees, 5 days 23 hours
          (sfc.index(35, 65, day), sfc.index(37, 68, day + hour * 6)), // 2x3 degrees, 6 hours
          (sfc.index(35, 65, day), sfc.index(40, 70, day + hour * 6)), // 5^2 degrees, 6 hours
          (sfc.index(39.999, 60.999, day + 3000), sfc.index(40.001, 61.001, day + 3120)), // small bounds
          (sfc.index(51.0, 51.0, 6000), sfc.index(51.1, 51.1, 6100)), // small bounds
          (sfc.index(51.0, 51.0, 30000), sfc.index(51.001, 51.001, 30100)), // small bounds
          (Z3(sfc.index(51.0, 51.0, 30000).z - 1), Z3(sfc.index(51.0, 51.0, 30000).z + 1)) // 62 bits in common
        )

        def print(l: Z3, u: Z3, size: Int): Unit =
          println(s"${round(sfc.invert(l))} ${round(sfc.invert(u))}\t$size")
        def round(z: (Double, Double, Long)): (Double, Double, Long) =
          (math.round(z._1 * 1000.0) / 1000.0, math.round(z._2 * 1000.0) / 1000.0, z._3)

        val start = System.currentTimeMillis()
        foreach(ranges) { r =>
          val ret = Z3.zranges(Array(ZRange(r._1, r._2)), maxRanges = Some(1000))
          ret.length must beGreaterThan(0)
        }
      }
    }
  }
}
