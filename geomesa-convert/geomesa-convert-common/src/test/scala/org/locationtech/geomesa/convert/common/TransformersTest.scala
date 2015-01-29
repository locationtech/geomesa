/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.convert.common

import java.util.Date

import com.google.common.hash.Hashing
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.Transformers
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TransformersTest extends Specification {

  "Transformers" should {

    implicit val ctx = new EvaluationContext(Map(), null)
    "handle transformations" >> {
      "handle string transformations" >> {

        "trim" >> {
          val exp = Transformers.parseTransform("trim($1)")
          exp.eval("", "foo ", "bar") must be equalTo "foo"
        }

        "capitalize" >> {
          val exp = Transformers.parseTransform("capitalize($1)")
          exp.eval("", "foo", "bar") must be equalTo "Foo"
        }

        "lowercase" >> {
          val exp = Transformers.parseTransform("lowercase($1)")
          exp.eval("", "FOO", "bar") must be equalTo "foo"
        }

        "regexReplace" >> {
          val exp = Transformers.parseTransform("regexReplace('foo'::r,'bar',$1)")
          exp.eval("", "foobar") must be equalTo "barbar"
        }

        "compound expression" >> {
          val exp = Transformers.parseTransform("regexReplace('foo'::r,'bar',trim($1))")
          exp.eval("", " foobar ") must be equalTo "barbar"
        }

        "substr" >> {
          val exp = Transformers.parseTransform("substr($1, 2, 5)")
          exp.eval("", "foobarbaz") must be equalTo "foobarbaz".substring(2, 5)
        }
      }

      "handle numeric literals" >> {
        val exp = Transformers.parseTransform("$2")
        exp.eval("", "1", 2) must be equalTo 2
      }

      "handle dates" >> {
        val testDate = DateTime.parse("2015-01-01T00:00:00.000Z").toDate

        "date with custom format" >> {
          val exp = Transformers.parseTransform("date('yyyyMMdd', $1)")
          exp.eval("", "20150101").asInstanceOf[Date] must be equalTo testDate
        }

        "isodate" >> {
          val exp = Transformers.parseTransform("isodate($1)")
          exp.eval("", "20150101").asInstanceOf[Date] must be equalTo testDate
        }

        "isodatetime" >> {
          val exp = Transformers.parseTransform("isodatetime($1)")
          exp.eval("", "20150101T000000.000Z").asInstanceOf[Date] must be equalTo testDate
        }

        "dateHourMinuteSecondMillis" >> {
          val exp = Transformers.parseTransform("dateHourMinuteSecondMillis($1)")
          exp.eval("", "2015-01-01T00:00:00.000").asInstanceOf[Date] must be equalTo testDate
        }

      }

      "handle point geometries" >> {
        val exp = Transformers.parseTransform("point($1, $2)")
        exp.eval("", 45.0, 45.0).asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(45.0, 45.0)
      }

      "handle identity functions" >> {
        val bytes = Array.ofDim[Byte](32)
        Random.nextBytes(bytes)
        "md5" >> {
          val hasher = Hashing.md5().newHasher()
          val exp = Transformers.parseTransform("md5($0)")
          val hashedResult = exp.eval(bytes).asInstanceOf[String]
          hashedResult must be equalTo hasher.putBytes(bytes).hash().toString
        }

        "uuid" >> {
          val exp = Transformers.parseTransform("uuid()")
          exp.eval(null) must anInstanceOf[String]
        }

        "base64" >> {
          val exp = Transformers.parseTransform("base64($0)")
          exp.eval(bytes) must be equalTo Base64.encodeBase64URLSafeString(bytes)
        }
      }

      "handle named values" >> {
        val closure = Array.ofDim[Any](3)
        closure(1) = "bar"
        implicit val ctx = new EvaluationContext(Map("foo" -> 1), closure)
        val exp = Transformers.parseTransform("capitalize($foo)")
        exp.eval(null)(ctx) must be equalTo "Bar"
      }
    }

    "handle predicates" >> {
      "string equals" >> {
        val exp = Transformers.parsePred("strEq($1, $2)")
        exp.eval("", "1", "2") must beFalse

        exp.eval("", "1", "1") must beTrue
      }

      "numeric predicates" >> {
        "int equals" >> {
          val exp = Transformers.parsePred("intEq($1::int, $2::int)")
          exp.eval("", "1", "2") must beFalse
          exp.eval("", "1", "1") must beTrue
        }

        "nested int equals" >> {
          val exp = Transformers.parsePred("intEq($1::int, strlen($2))")
          exp.eval("", "3", "foo") must beTrue
          exp.eval("", "4", "foo") must beFalse
        }

        "int lteq" >> {
          val exp = Transformers.parsePred("intLTEq($1::int, $2::int)")
          exp.eval("", "1", "2") must beTrue
          exp.eval("", "1", "1") must beTrue
          exp.eval("", "1", "0") must beFalse
        }
        "int lt" >> {
          val exp = Transformers.parsePred("intLT($1::int, $2::int)")
          exp.eval("", "1", "2") must beTrue
          exp.eval("", "1", "1") must beFalse
        }
        "int gteq" >> {
          val exp = Transformers.parsePred("intGTEq($1::int, $2::int)")
          exp.eval("", "1", "2") must beFalse
          exp.eval("", "1", "1") must beTrue
          exp.eval("", "2", "1") must beTrue
        }
        "int gt" >> {
          val exp = Transformers.parsePred("intGT($1::int, $2::int)")
          exp.eval("", "1", "2") must beFalse
          exp.eval("", "1", "1") must beFalse
          exp.eval("", "2", "1") must beTrue
        }
        "double equals" >> {
          val exp = Transformers.parsePred("dEq($1::double, $2::double)")
          exp.eval("", "1.0", "2.0") must beFalse
          exp.eval("", "1.0", "1.0") must beTrue
        }

        "double lteq" >> {
          val exp = Transformers.parsePred("dLTEq($1::double, $2::double)")
          exp.eval("", "1.0", "2.0") must beTrue
          exp.eval("", "1.0", "1.0") must beTrue
          exp.eval("", "1.0", "0.0") must beFalse
        }
        "double lt" >> {
          val exp = Transformers.parsePred("dLT($1::double, $2::double)")
          exp.eval("", "1.0", "2.0") must beTrue
          exp.eval("", "1.0", "1.0") must beFalse
        }
        "double gteq" >> {
          val exp = Transformers.parsePred("dGTEq($1::double, $2::double)")
          exp.eval("", "1.0", "2.0") must beFalse
          exp.eval("", "1.0", "1.0") must beTrue
          exp.eval("", "2.0", "1.0") must beTrue
        }
        "double gt" >> {
          val exp = Transformers.parsePred("dGT($1::double, $2::double)")
          exp.eval("", "1.0", "2.0") must beFalse
          exp.eval("", "1.0", "1.0") must beFalse
          exp.eval("", "2.0", "1.0") must beTrue
        }

      }

      "logic predicates" >> {
        "not" >> {
          val exp = Transformers.parsePred("not(strEq($1, $2))")
          exp.eval("", "1", "1") must beFalse
        }

        "and" >> {
          val exp = Transformers.parsePred("and(strEq($1, $2), strEq(concat($3, $4), $1))")
          exp.eval("", "foo", "foo", "f", "oo") must beTrue
        }

        "or" >> {
          val exp = Transformers.parsePred("or(strEq($1, $2), strEq($3, $1))")
          exp.eval("", "foo", "foo", "f", "oo") must beTrue
        }
      }
    }
  }
}
