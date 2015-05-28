/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.csv

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

import DMS._

case class DMS(degrees: Int, minutes: Int, seconds: Double, hemisphere: Hemisphere) {
  require(0 <= seconds && seconds < 60, "Seconds must be between 0 and 60")
  require(0 <= minutes && minutes < 60, "Minutes must be between 0 and 60")
  require(0 <= degrees && toDouble < hemisphere.maxDeg,
          s"Degrees must be between 0 and ${hemisphere.maxDeg}")

  lazy val toDouble: Double = hemisphere.sign * (degrees + ((minutes + (seconds / 60)) / 60))
  override lazy val toString: String = f"$degrees%d:$minutes%02d:$seconds%05.2f${hemisphere.char}%s"
}
