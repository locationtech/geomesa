/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.csv

// TODO: replace with org.geotools.measure.AngleFormat
object DMS {
  object Hemisphere {
    def apply(char: Char): Hemisphere = char.toUpper match {
      case 'N' => North
      case 'S' => South
      case 'E' => East
      case 'W' => West
      case _   => throw new IllegalArgumentException(s"Unrecognized hemisphere \'$char\'")
    }
  }

  sealed trait Hemisphere {
    def char: Char
    def sign: Int
    def maxDeg: Int
    def opposite: Hemisphere
  }
  sealed trait LatHemi extends Hemisphere { val maxDeg = 90  }
  sealed trait LonHemi extends Hemisphere { val maxDeg = 180 }
  case object North extends LatHemi { val char = 'N'; val sign =  1; val opposite = South }
  case object South extends LatHemi { val char = 'S'; val sign = -1; val opposite = North }
  case object East  extends LonHemi { val char = 'E'; val sign =  1; val opposite = West  }
  case object West  extends LonHemi { val char = 'W'; val sign = -1; val opposite = East  }

  val regex = "(-?)(\\d{2,3}):?(\\d{2}):?(\\d{2}(?:\\.\\d+)?)([NnSsEeWw])".r
  def apply(dmsStr: String): DMS = dmsStr match {
    case regex(sign, degS, minS, secS, hemiS) =>
      val h = if (sign.isEmpty) Hemisphere(hemiS.head) else Hemisphere(hemiS.head).opposite
      val d = degS.toInt
      val m = minS.toInt
      val s = secS.toDouble
      DMS(d,m,s,h)
    case _ => throw new IllegalArgumentException(s"Cannot parse $dmsStr as DMS")
  }
}

import org.locationtech.geomesa.utils.csv.DMS._

case class DMS(degrees: Int, minutes: Int, seconds: Double, hemisphere: Hemisphere) {
  require(0 <= seconds && seconds < 60, "Seconds must be between 0 and 60")
  require(0 <= minutes && minutes < 60, "Minutes must be between 0 and 60")
  require(0 <= degrees && toDouble < hemisphere.maxDeg,
          s"Degrees must be between 0 and ${hemisphere.maxDeg}")

  lazy val toDouble: Double = hemisphere.sign * (degrees + ((minutes + (seconds / 60)) / 60))
  override lazy val toString: String = f"$degrees%d:$minutes%02d:$seconds%05.2f${hemisphere.char}%s"
}
