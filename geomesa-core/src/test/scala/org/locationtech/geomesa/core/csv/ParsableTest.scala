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
import org.locationtech.geomesa.core.csv.Parsable._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Success

@RunWith(classOf[JUnitRunner])
class ParsableTest extends Specification {

  val i = 1
  val d = 1.0
  val time = new DateTime
  val pointStr = s"POINT(0.0 0.0)"
  val s = "argle"

  override def is =
    "IntIsParsable"    ! {
      IntIsParsable.parse(i.toString) == Success(new jInt(i))
    } ^
    "DoubleIsParsable" ! {
      DoubleIsParsable.parse(d.toString) == Success(new jDouble(d))
    } ^
    "TimeIsParsable"   ! {
      TimeIsParsable.timeFormats.forall(f =>
        TimeIsParsable.parse(f.print(time)).map(_.getTime / 1000) == Success(time.getMillis / 1000)
      )
    } ^
    "PointIsParsable"  ! {
      PointIsParsable.parse(pointStr) == Success(WKTUtils.read(pointStr))
    } ^
    "StringIsParsable" ! {
      StringIsParsable.parse(s) == Success(s)
    }
}
