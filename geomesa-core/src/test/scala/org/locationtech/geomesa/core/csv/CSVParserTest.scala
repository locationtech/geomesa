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

package org.locationtech.geomesa.core.csv

import java.lang.{Integer => jInt, Double => jDouble}

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.csv.DMS.North
import org.locationtech.geomesa.core.csv.CSVParser._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Success

@RunWith(classOf[JUnitRunner])
class CSVParserTest extends Specification {

  val i = 1
  val d = 1.0
  val dms = DMS(38,4,31.17,North)
  val time = new DateTime
  val pointStr = "POINT(0.0 0.0)"
  val dmsPtStr = "38:04:31.17N -78:29:42.32E"
  val dmsPtX = -78.495089
  val dmsPtY =  38.075325
  val s = "argle"

  val eps = 0.000001

  override def is =
    "IntIsParsable"    ! {
      IntParser.parse(i.toString) == Success(new jInt(i))
    } ^
    "DoubleIsParsable" ! {
      DoubleParser.parse(d.toString) == Success(new jDouble(d))
    } ^
    "DoubleIsParsable handles DMS" ! {
      DoubleParser.parse(dms.toString) == Success(new jDouble(dms.toDouble))
    } ^
    "TimeIsParsable"   ! {
      TimeParser.timeFormats.forall(f =>
        TimeParser.parse(f.print(time)).map(_.getTime / 1000) == Success(time.getMillis / 1000)
      )
    } ^
    "PointIsParsable"  ! {
      PointParser.parse(pointStr) == Success(WKTUtils.read(pointStr))
    } ^
    "PointIsParsable handles DMS" ! {
      val Success(resultPt) = PointParser.parse(dmsPtStr)
      math.abs(resultPt.getX - dmsPtX) < eps
      math.abs(resultPt.getY - dmsPtY) < eps
    } ^
    "StringIsParsable" ! {
      StringParser.parse(s) == Success(s)
    }
}
