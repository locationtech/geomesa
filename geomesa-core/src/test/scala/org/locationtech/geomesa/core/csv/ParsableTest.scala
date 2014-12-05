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
import org.joda.time.format.DateTimeFormatter
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.csv.Parsable._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Success

@RunWith(classOf[JUnitRunner])
class ParsableTest extends Specification with ScalaCheck {
  import CSVTestUtils.{randomDate, randomFormat}

  val intParsable    = Prop.forAll { (i: Int)    =>
    val s = i.toString
    IntIsParsable.parse(s)        == Success(new jInt(i))
                                   }
  val doubleParsable = Prop.forAll { (d: Double) =>
    val s = d.toString
    DoubleIsParsable.parse(s)     == Success(new jDouble(d))
                                   }
  val timeParsable   = Prop.forAll { (d: DateTime, f: DateTimeFormatter) =>
    val s = f.print(d)
    TimeIsParsable.parse(s)
                  .map(_.getTime / 1000) == Success(d.getMillis / 1000)
                                   }
  val pointParsable  = Prop.forAll { (lat: Double, lon: Double) =>
    val s = s"POINT($lon $lat)"
    PointIsParsable.parse(s)      == Success(WKTUtils.read(s))
                                   }
  val stringParsable = Prop.forAll { (s: String) =>
    StringIsParsable.parse(s)     == Success(s)
                                   }

  override def is =
    "IntIsParsable"    ! check { intParsable } ^
    "DoubleIsParsable" ! check { doubleParsable } ^
    "TimeIsParsable"   ! check { timeParsable } ^
    "PointIsParsable"  ! check { pointParsable } ^
    "StringIsParsable" ! check { stringParsable }
}
