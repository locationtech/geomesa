/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.common

import java.util.Date

import com.google.common.hash.Hashing
import com.vividsolutions.jts.geom._
import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.Transformers
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Predicate}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TransformersTest extends Specification {

  "Transformers" should {

    implicit val ctx = new EvaluationContext(mutable.HashMap.empty[String, Int], null)
    "handle transformations" >> {
      "handle string transformations" >> {

        "allow literal strings" >> {
          val exp = Transformers.parseTransform("'hello'")
          exp.eval(Array(null)) must be equalTo "hello"
        }

        "allow quoted strings" >> {
          val exp = Transformers.parseTransform("'he\\'llo'")
          exp.eval(Array(null)) must be equalTo "he'llo"
        }

        "not parse non-quoted things (this shouldn't be a string)" >> {
          val exp = Transformers.parseTransform("5")
          exp.eval(Array(null)) must be equalTo 5
        }
        "allow empty literal strings" >> {
          val exp = Transformers.parseTransform("''")
          exp.eval(Array(null)) must be equalTo ""
        }

        "trim" >> {
          val exp = Transformers.parseTransform("trim($1)")
          exp.eval(Array("", "foo ", "bar")) must be equalTo "foo"
        }

        "capitalize" >> {
          val exp = Transformers.parseTransform("capitalize($1)")
          exp.eval(Array("", "foo", "bar")) must be equalTo "Foo"
        }

        "lowercase" >> {
          val exp = Transformers.parseTransform("lowercase($1)")
          exp.eval(Array("", "FOO", "bar")) must be equalTo "foo"
        }

        "regexReplace" >> {
          val exp = Transformers.parseTransform("regexReplace('foo'::r,'bar',$1)")
          exp.eval(Array("", "foobar")) must be equalTo "barbar"
        }

        "compound expression" >> {
          val exp = Transformers.parseTransform("regexReplace('foo'::r,'bar',trim($1))")
          exp.eval(Array("", " foobar ")) must be equalTo "barbar"
        }

        "substr" >> {
          val exp = Transformers.parseTransform("substr($1, 2, 5)")
          exp.eval(Array("", "foobarbaz")) must be equalTo "foobarbaz".substring(2, 5)
        }
      }

      "handle numeric literals" >> {
        val exp = Transformers.parseTransform("$2")
        exp.eval(Array("", "1", 2)) must be equalTo 2
      }

      "handle dates" >> {
        val testDate = DateTime.parse("2015-01-01T00:00:00.000Z").toDate

        "date with custom format" >> {
          val exp = Transformers.parseTransform("date('yyyyMMdd', $1)")
          exp.eval(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
        }

        "date with a realistic custom format" >> {
          val exp = Transformers.parseTransform("date('YYYY-MM-dd\\'T\\'HH:mm:ss.SSSSSS', $1)")
          exp.eval(Array("", "2015-01-01T00:00:00.000000")).asInstanceOf[Date] must be equalTo testDate
        }

        "datetime" >> {
          val exp = Transformers.parseTransform("datetime($1)")
          exp.eval(Array("", "2015-01-01T00:00:00.000Z")).asInstanceOf[Date] must be equalTo testDate
        }

        "isodate" >> {
          val exp = Transformers.parseTransform("isodate($1)")
          exp.eval(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
        }

        "isodatetime" >> {
          val exp = Transformers.parseTransform("isodatetime($1)")
          exp.eval(Array("", "20150101T000000.000Z")).asInstanceOf[Date] must be equalTo testDate
        }

        "dateHourMinuteSecondMillis" >> {
          val exp = Transformers.parseTransform("dateHourMinuteSecondMillis($1)")
          exp.eval(Array("", "2015-01-01T00:00:00.000")).asInstanceOf[Date] must be equalTo testDate
        }

      }

      "handle point geometries" >> {
        val exp = Transformers.parseTransform("point($1, $2)")
        exp.eval(Array("", 45.0, 45.0)).asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(45.0, 45.0)

        val trans = Transformers.parseTransform("point($0)")
        trans.eval(Array("POINT(50 52)")).asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(50, 52)

        // turn "Geometry" into "Point"
        val geoFac = new GeometryFactory()
        val geom = geoFac.createPoint(new Coordinate(55, 56)).asInstanceOf[Geometry]
        val res = trans.eval(Array(geom))
        res must beAnInstanceOf[Point]
        res.asInstanceOf[Point] mustEqual geoFac.createPoint(new Coordinate(55, 56))
      }

      "handle linestring wkt" >> {
        val geoFac = new GeometryFactory()
        val lineStr = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        val trans = Transformers.parseTransform("linestring($0)")
        trans.eval(Array("Linestring(102 0, 103 1, 104 0, 105 1)")).asInstanceOf[LineString] must be equalTo lineStr

        // type conversion
        val geom = lineStr.asInstanceOf[Geometry]
        val res = trans.eval(Array(geom))
        res must beAnInstanceOf[LineString]
        res.asInstanceOf[LineString] mustEqual WKTUtils.read("Linestring(102 0, 103 1, 104 0, 105 1)")
      }

      "handle polygon wkt" >> {
        val geoFac = new GeometryFactory()
        val poly = geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        val trans = Transformers.parseTransform("polygon($0)")
        trans.eval(Array("polygon((100 0, 101 0, 101 1, 100 1, 100 0))")).asInstanceOf[Polygon] must be equalTo poly

        // type conversion
        val geom = poly.asInstanceOf[Polygon]
        val res = trans.eval(Array(geom))
        res must beAnInstanceOf[Polygon]
        res.asInstanceOf[Polygon] mustEqual WKTUtils.read("polygon((100 0, 101 0, 101 1, 100 1, 100 0))")
      }

      "handle geometry wkt" >> {
        val geoFac = new GeometryFactory()
        val lineStr = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        val trans = Transformers.parseTransform("geometry($0)")
        trans.eval(Array("Linestring(102 0, 103 1, 104 0, 105 1)")).asInstanceOf[Geometry] must be equalTo lineStr
      }

      "handle identity functions" >> {
        val bytes = Array.ofDim[Byte](32)
        Random.nextBytes(bytes)
        "md5" >> {
          val hasher = Hashing.md5().newHasher()
          val exp = Transformers.parseTransform("md5($0)")
          val hashedResult = exp.eval(Array(bytes)).asInstanceOf[String]
          hashedResult must be equalTo hasher.putBytes(bytes).hash().toString
        }

        "uuid" >> {
          val exp = Transformers.parseTransform("uuid()")
          exp.eval(Array(null)) must anInstanceOf[String]
        }

        "base64" >> {
          val exp = Transformers.parseTransform("base64($0)")
          exp.eval(Array(bytes)) must be equalTo Base64.encodeBase64URLSafeString(bytes)
        }
      }

      "handle named values" >> {
        val closure = Array.ofDim[Any](3)
        closure(1) = "bar"
        implicit val ctx = new EvaluationContext(mutable.HashMap("foo" -> 1), closure)
        val exp = Transformers.parseTransform("capitalize($foo)")
        exp.eval(Array(null))(ctx) must be equalTo "Bar"
      }
    }

    "handle predicates" >> {
      "string equals" >> {
        val exp = Transformers.parsePred("strEq($1, $2)")
        exp.eval(Array("", "1", "2")) must beFalse

        exp.eval(Array("", "1", "1")) must beTrue
      }

      "numeric predicates" >> {
        "int equals" >> {
          val exp = Transformers.parsePred("intEq($1::int, $2::int)")
          exp.eval(Array("", "1", "2")) must beFalse
          exp.eval(Array("", "1", "1")) must beTrue
        }

        "nested int equals" >> {
          val exp = Transformers.parsePred("intEq($1::int, strlen($2))")
          exp.eval(Array("", "3", "foo")) must beTrue
          exp.eval(Array("", "4", "foo")) must beFalse
        }

        "int lteq" >> {
          val exp = Transformers.parsePred("intLTEq($1::int, $2::int)")
          exp.eval(Array("", "1", "2")) must beTrue
          exp.eval(Array("", "1", "1")) must beTrue
          exp.eval(Array("", "1", "0")) must beFalse
        }
        "int lt" >> {
          val exp = Transformers.parsePred("intLT($1::int, $2::int)")
          exp.eval(Array("", "1", "2")) must beTrue
          exp.eval(Array("", "1", "1")) must beFalse
        }
        "int gteq" >> {
          val exp = Transformers.parsePred("intGTEq($1::int, $2::int)")
          exp.eval(Array("", "1", "2")) must beFalse
          exp.eval(Array("", "1", "1")) must beTrue
          exp.eval(Array("", "2", "1")) must beTrue
        }
        "int gt" >> {
          val exp = Transformers.parsePred("intGT($1::int, $2::int)")
          exp.eval(Array("", "1", "2")) must beFalse
          exp.eval(Array("", "1", "1")) must beFalse
          exp.eval(Array("", "2", "1")) must beTrue
        }
        "double equals" >> {
          val exp = Transformers.parsePred("doubleEq($1::double, $2::double)")
          exp.eval(Array("", "1.0", "2.0")) must beFalse
          exp.eval(Array("", "1.0", "1.0")) must beTrue
        }

        "double lteq" >> {
          val exp = Transformers.parsePred("doubleLTEq($1::double, $2::double)")
          exp.eval(Array("", "1.0", "2.0")) must beTrue
          exp.eval(Array("", "1.0", "1.0")) must beTrue
          exp.eval(Array("", "1.0", "0.0")) must beFalse
        }
        "double lt" >> {
          val exp = Transformers.parsePred("doubleLT($1::double, $2::double)")
          exp.eval(Array("", "1.0", "2.0")) must beTrue
          exp.eval(Array("", "1.0", "1.0")) must beFalse
        }
        "double gteq" >> {
          val exp = Transformers.parsePred("doubleGTEq($1::double, $2::double)")
          exp.eval(Array("", "1.0", "2.0")) must beFalse
          exp.eval(Array("", "1.0", "1.0")) must beTrue
          exp.eval(Array("", "2.0", "1.0")) must beTrue
        }
        "double gt" >> {
          val exp = Transformers.parsePred("doubleGT($1::double, $2::double)")
          exp.eval(Array("", "1.0", "2.0")) must beFalse
          exp.eval(Array("", "1.0", "1.0")) must beFalse
          exp.eval(Array("", "2.0", "1.0")) must beTrue
        }
      }

      "string2 functions" >> {
        "string2double" >> {
          "double string2double zero default" >> {
            val exp: Predicate = Transformers.parsePred("doubleEq(string2double($1, $3), string2double($2, $3))")
            exp.eval(Array("", "1.2", "1.0", 0.0)) must beFalse
            exp.eval(Array("", "0.0", "0.0", 0.0)) must beTrue
          }
          "double empty string string2double zero default" >> {
            val exp: Predicate = Transformers.parsePred("doubleEq(string2double($1, $3), string2double($2, $3))")
            exp.eval(Array("", "", "1.0", 0.0)) must beFalse
            exp.eval(Array("", "", "0.0", 0.0)) must beTrue
          }
          "double null string string2double zero default" >> {
            val exp = Transformers.parsePred("doubleEq(string2double($1,$3), string2double($2,$3))")
            exp.eval(Array("", null, "1.0", 0.0)) must beFalse
            exp.eval(Array("", null, "0.0", 0.0)) must beTrue
          }
          "double string2double null default" >> {
            val exp: Predicate = Transformers.parsePred("doubleEq(string2double($1, $3), string2double($2, $3))")
            exp.eval(Array("", "1.2", "1.0", null)) must beFalse
            exp.eval(Array("", "0.0", "0.0", null)) must beTrue
          }
          "double empty string string2double null default" >> {
            val exp: Predicate = Transformers.parsePred("doubleEq(string2double($1, $3), string2double($2, $3))")
            exp.eval(Array("", "", "1.0", null)) must beFalse
            exp.eval(Array("", "", null, null)) must beTrue
          }
          "double null string string2double null default" >> {
            val exp = Transformers.parsePred("doubleEq(string2double($1,$3), string2double($2,$3))")
            exp.eval(Array("", null, "1.0", null)) must beFalse
            exp.eval(Array("", null, null, null)) must beTrue
          }
        }
        "string2int" >> {
          "int string2int zero default" >> {
            val exp: Predicate = Transformers.parsePred("intEq(string2int($1, $3), string2int($2, $3))")
            exp.eval(Array("", "2", "1", 0)) must beFalse
            exp.eval(Array("", "0", "0", 0)) must beTrue
          }
          "int empty string string2int zero default" >> {
            val exp: Predicate = Transformers.parsePred("intEq(string2int($1, $3), string2int($2, $3))")
            exp.eval(Array("", "", "1", 0)) must beFalse
            exp.eval(Array("", "", "0", 0)) must beTrue
          }
          "int null string string2int zero default" >> {
            val exp = Transformers.parsePred("intEq(string2int($1,$3), string2int($2,$3))")
            exp.eval(Array("", null, "1", 0)) must beFalse
            exp.eval(Array("", null, "0", 0)) must beTrue
          }
          "int string2int null default" >> {
            val exp: Predicate = Transformers.parsePred("intEq(string2int($1, $3), string2int($2, $3))")
            exp.eval(Array("", "2", "1", null)) must beFalse
            exp.eval(Array("", "0", "0", null)) must beTrue
          }
          "int empty string string2int null default" >> {
            val exp: Predicate = Transformers.parsePred("intEq(string2int($1, $3), string2int($2, $3))")
            exp.eval(Array("", "", "1", null)) must beFalse
            exp.eval(Array("", "", null, null)) must beTrue
          }
          "int null string string2int null default" >> {
            val exp = Transformers.parsePred("intEq(string2int($1,$3), string2int($2,$3))")
            exp.eval(Array("", null, "1", null)) must beFalse
            exp.eval(Array("", null, null, null)) must beTrue
          }
        }
        "string2long" >> {
          "long string2long zero default" >> {
            val exp: Predicate = Transformers.parsePred("longEq(string2long($1, $3), string2long($2, $3))")
            exp.eval(Array("", "22960000000", "12960000000", 0L)) must beFalse
            exp.eval(Array("", "0", "0", 0L)) must beTrue
          }
          "long empty string string2long zero default" >> {
            val exp: Predicate = Transformers.parsePred("longEq(string2long($1, $3), string2long($2, $3))")
            exp.eval(Array("", "", "12960000000", 0L)) must beFalse
            exp.eval(Array("", "", "0", 0L)) must beTrue
          }
          "long null string string2long zero default" >> {
            val exp = Transformers.parsePred("longEq(string2long($1,$3), string2long($2,$3))")
            exp.eval(Array("", null, "12960000000", 0L)) must beFalse
            exp.eval(Array("", null, "0", 0L)) must beTrue
          }
          "long string2long null default" >> {
            val exp: Predicate = Transformers.parsePred("longEq(string2long($1, $3), string2long($2, $3))")
            exp.eval(Array("", "22960000000", "12960000000", null)) must beFalse
            exp.eval(Array("", "0", "0", null)) must beTrue
          }
          "long empty string string2long null default" >> {
            val exp: Predicate = Transformers.parsePred("longEq(string2long($1, $3), string2long($2, $3))")
            exp.eval(Array("", "", "12960000000", null)) must beFalse
            exp.eval(Array("", "", null, null)) must beTrue
          }
          "long null string string2long null default" >> {
            val exp = Transformers.parsePred("longEq(string2long($1,$3), string2long($2,$3))")
            exp.eval(Array("", null, "12960000000", null)) must beFalse
            exp.eval(Array("", null, null, null)) must beTrue
          }
        }
        "string2float" >> {
          "float string2float zero default" >> {
            val exp: Predicate = Transformers.parsePred("floatEq(string2float($1, $3), string2float($2, $3))")
            exp.eval(Array("", "1.2", "1.0", 0.0f)) must beFalse
            exp.eval(Array("", "0.0", "0.0", 0.0f)) must beTrue
          }
          "float empty string string2float zero default" >> {
            val exp: Predicate = Transformers.parsePred("floatEq(string2float($1, $3), string2float($2, $3))")
            exp.eval(Array("", "", "1.0", 0.0f)) must beFalse
            exp.eval(Array("", "", "0.0", 0.0f)) must beTrue
          }
          "float null string string2float zero default" >> {
            val exp = Transformers.parsePred("floatEq(string2float($1,$3), string2float($2,$3))")
            exp.eval(Array("", null, "1.0", 0.0f)) must beFalse
            exp.eval(Array("", null, "0.0", 0.0f)) must beTrue
          }
          "float string2float null default" >> {
            val exp: Predicate = Transformers.parsePred("floatEq(string2float($1, $3), string2float($2, $3))")
            exp.eval(Array("", "1.2", "1.0", null)) must beFalse
            exp.eval(Array("", "0.0", "0.0", null)) must beTrue
          }
          "float empty string string2float null default" >> {
            val exp: Predicate = Transformers.parsePred("floatEq(string2float($1, $3), string2float($2, $3))")
            exp.eval(Array("", "", "1.0", null)) must beFalse
            exp.eval(Array("", "", null, null)) must beTrue
          }
          "float null string string2float null default" >> {
            val exp = Transformers.parsePred("floatEq(string2float($1,$3), string2float($2,$3))")
            exp.eval(Array("", null, "1.0", null)) must beFalse
            exp.eval(Array("", null, null, null)) must beTrue
          }
        }
        "string2boolean" >> {
          "boolean string2boolean true default" >> {
            val exp = Transformers.parseTransform("string2boolean($1,$2)")
            exp.eval(Array("", "true", true)) mustEqual true
            exp.eval(Array("", "false", true)) mustEqual false
            exp.eval(Array("", "", true)) mustEqual true
            exp.eval(Array("", null, true)) mustEqual true
          }
          "boolean string2boolean false default" >> {
            val exp = Transformers.parseTransform("string2boolean($1,$2)")
            exp.eval(Array("", "true", false)) mustEqual true
            exp.eval(Array("", "false", false)) mustEqual false
            exp.eval(Array("", "", false)) mustEqual false
            exp.eval(Array("", null, false)) mustEqual false
          }
          "boolean string2boolean null default" >> {
            val exp = Transformers.parseTransform("string2boolean($1,$2)")
            exp.eval(Array("", "true", null)) mustEqual true
            exp.eval(Array("", "false", null)) mustEqual false
            exp.eval(Array("", "", null)) mustEqual null
            exp.eval(Array("", null, null)) mustEqual null
          }
        }
      }

      "logic predicates" >> {
        "not" >> {
          val exp = Transformers.parsePred("not(strEq($1, $2))")
          exp.eval(Array("", "1", "1")) must beFalse
        }

        "and" >> {
          val exp = Transformers.parsePred("and(strEq($1, $2), strEq(concat($3, $4), $1))")
          exp.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
        }

        "or" >> {
          val exp = Transformers.parsePred("or(strEq($1, $2), strEq($3, $1))")
          exp.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
        }
      }
    }

    import scala.collection.JavaConversions._
    "create lists" >> {
      val trans = Transformers.parseTransform("list($0, $1, $2)")
      val res = trans.eval(Array("a", "b", "c")).asInstanceOf[java.util.List[String]]
      res.size() mustEqual 3
      res.toList must containTheSameElementsAs(List("a", "b", "c"))
    }

    "parse lists" >> {
      "default delimiter" >> {
        val trans = Transformers.parseTransform("parseList('string', $0)")
        val res = trans.eval(Array("a,b,c")).asInstanceOf[java.util.List[String]]
        res.size mustEqual 3
        res.toList must containTheSameElementsAs(List("a", "b", "c"))
      }
      "custom delimiter" >> {
        val trans = Transformers.parseTransform("parseList('string', $0, '%')")
        val res = trans.eval(Array("a%b%c")).asInstanceOf[java.util.List[String]]
        res.size mustEqual 3
        res.toList must containTheSameElementsAs(List("a", "b", "c"))
      }
      "with numbers" >> {
        val trans = Transformers.parseTransform("parseList('int', $0, '%')")
        val res = trans.eval(Array("1%2%3")).asInstanceOf[java.util.List[Int]]
        res.size mustEqual 3
        res.toList must containTheSameElementsAs(List(1,2,3))
      }
      "with numbers" >> {
        val trans = Transformers.parseTransform("parseList('int', $0, '%')")
        trans.eval(Array("1%2%a")).asInstanceOf[java.util.List[Int]] must throwAn[IllegalArgumentException]
      }
    }
  }
}
