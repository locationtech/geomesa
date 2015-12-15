/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Z3Test extends Specification {

  val rand = new Random(-574)
  val maxInt = Z3SFC.lon.precision.toInt
  def nextDim() = rand.nextInt(maxInt)

  def padTo(s: String) = (new String(Array.fill(63)('0')) + s).takeRight(63)

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
      val z3curve = Z3SFC
      val (x, y, t) = (z3curve.lon.precision, z3curve.lat.precision, z3curve.time.precision)
      val z = Z3(x.toInt, y.toInt, t.toInt)
      z match { case Z3(zx, zy, zt) =>
        zx mustEqual x
        zy mustEqual y
        zt mustEqual t
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
  }
}
