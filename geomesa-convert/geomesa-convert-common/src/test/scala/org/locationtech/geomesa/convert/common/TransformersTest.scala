/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.common

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date

import com.google.common.hash.Hashing
import org.locationtech.jts.geom._
import org.apache.commons.codec.binary.Base64
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.{EvaluationContext, EvaluationContextImpl, Transformers}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TransformersTest extends Specification {

  "Transformers" should {

    implicit val ctx: EvaluationContext = EvaluationContext.empty

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
        "allow empty literal strings" >> {
          val exp = Transformers.parseTransform("''")
          exp.eval(Array(null)) must be equalTo ""
        }
        "allow native ints" >> {
          val res = Transformers.parseTransform("1").eval(Array(null))
          res must not(beNull)
          res.getClass mustEqual classOf[java.lang.Integer]
          res mustEqual 1
        }
        "allow native longs" >> {
          val res = Transformers.parseTransform("1L").eval(Array(null))
          res must not(beNull)
          res.getClass mustEqual classOf[java.lang.Long]
          res mustEqual 1L
        }
        "allow native floats" >> {
          val tests = Seq(("1.0", 1f), ("1.0", 1f), (".1", .1f), ("0.1", .1f), ("-1.0", -1f))
          foreach(tests) { case (s, expected) =>
            foreach(Seq("f", "F")) { suffix =>
              val res = Transformers.parseTransform(s + suffix).eval(Array(null))
              res must not(beNull)
              res.getClass mustEqual classOf[java.lang.Float]
              res mustEqual expected
            }
          }
        }
        "allow native doubles" >> {
          val tests = Seq(("1.0", 1d), ("0.1", 0.1d), (".1", 0.1d), ("-1.0", -1d), ("-0.1", -0.1d))
          foreach(tests) { case (s, expected) =>
            foreach(Seq("", "d", "D")) { suffix =>
              val res = Transformers.parseTransform(s + suffix).eval(Array(null))
              res must not(beNull)
              res.getClass mustEqual classOf[java.lang.Double]
              res mustEqual expected
            }
          }
        }
        "allow native booleans" >> {
          Transformers.parseTransform("false").eval(Array(null)) mustEqual false
          Transformers.parseTransform("true").eval(Array(null)) mustEqual true
        }
        "allow native nulls" >> {
          Transformers.parseTransform("null").eval(Array(null)) must beNull
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
        "uppercase" >> {
          val exp = Transformers.parseTransform("uppercase($1)")
          exp.eval(Array("", "FoO")) must be equalTo "FOO"
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
        "substring" >> {
          val exp = Transformers.parseTransform("substring($1, 2, 5)")
          exp.eval(Array("", "foobarbaz")) must be equalTo "foobarbaz".substring(2, 5)
        }
        "strlen" >> {
          val exp = Transformers.parseTransform("strlen($1)")
          exp.eval(Array("", "FOO")) must be equalTo 3
        }
        "length" >> {
          val exp = Transformers.parseTransform("length($1)")
          exp.eval(Array("", "FOO")) must be equalTo 3
        }
        "toString" >> {
          val exp = Transformers.parseTransform("toString($1)")
          exp.eval(Array("", 5)) must be equalTo "5"
        }
        "concat with tostring" >> {
          val exp = Transformers.parseTransform("concat(toString($1), toString($2))")
          exp.eval(Array("", 5, 6)) must be equalTo "56"
        }
        "concat many args" >> {
          val exp = Transformers.parseTransform("concat($1, $2, $3, $4, $5, $6)")
          exp.eval(Array("", 1, 2, 3, 4, 5, 6)) must be equalTo "123456"
        }
        "mkstring" >> {
          val exp = Transformers.parseTransform("mkstring(',', $1, $2, $3, $4, $5, $6)")
          exp.eval(Array("", 1, 2, 3, 4, 5, 6)) must be equalTo "1,2,3,4,5,6"
        }
        "emptyToNull" >> {
          val exp = Transformers.parseTransform("emptyToNull($1)")
          exp.eval(Array("", "foo")) mustEqual "foo"
          exp.eval(Array("", "")) must beNull
          exp.eval(Array("", "  ")) must beNull
          exp.eval(Array("", null)) must beNull
        }
        "printf" >> {
          val exp = Transformers.parseTransform("printf('%s-%s-%sT00:00:00.000Z', '2015', '01', '01')")
          exp.eval(Array()) mustEqual "2015-01-01T00:00:00.000Z"
        }
      }

      "handle non-string literals" >> {
        "input is an int" >> {
          val exp = Transformers.parseTransform("$2")
          exp.eval(Array("", "1", 2)) must be equalTo 2
        }
        "cast to int" >> {
          val exp = Transformers.parseTransform("$1::int")
          exp.eval(Array("", "1")) mustEqual 1
          exp.eval(Array("", 1E2)) mustEqual 100
          exp.eval(Array("", 1)) mustEqual 1
          exp.eval(Array("", 1D)) mustEqual 1
          exp.eval(Array("", 1F)) mustEqual 1
          exp.eval(Array("", 1L)) mustEqual 1
        }
        "cast to integer" >> {
          val exp = Transformers.parseTransform("$1::integer")
          exp.eval(Array("", "1")) mustEqual 1
          exp.eval(Array("", 1E2)) mustEqual 100
          exp.eval(Array("", 1)) mustEqual 1
          exp.eval(Array("", 1D)) mustEqual 1
          exp.eval(Array("", 1F)) mustEqual 1
          exp.eval(Array("", 1L)) mustEqual 1
        }
        "cast to long" >> {
          val exp = Transformers.parseTransform("$1::long")
          exp.eval(Array("", "1")) mustEqual 1L
          exp.eval(Array("", 1E2)) mustEqual 100L
          exp.eval(Array("", 1)) mustEqual 1L
          exp.eval(Array("", 1D)) mustEqual 1L
          exp.eval(Array("", 1F)) mustEqual 1L
          exp.eval(Array("", 1L)) mustEqual 1L
        }
        "cast to float" >> {
          val exp = Transformers.parseTransform("$1::float")
          exp.eval(Array("", "1")) mustEqual 1F
          exp.eval(Array("", 1E2)) mustEqual 100F
          exp.eval(Array("", 1)) mustEqual 1F
          exp.eval(Array("", 1D)) mustEqual 1F
          exp.eval(Array("", 1F)) mustEqual 1F
          exp.eval(Array("", 1L)) mustEqual 1F
        }
        "cast to double" >> {
          val exp = Transformers.parseTransform("$1::double")
          exp.eval(Array("", "1")) mustEqual 1D
          exp.eval(Array("", 1E2)) mustEqual 100D
          exp.eval(Array("", 1)) mustEqual 1D
          exp.eval(Array("", 1D)) mustEqual 1D
          exp.eval(Array("", 1F)) mustEqual 1D
          exp.eval(Array("", 1L)) mustEqual 1D
        }
        "cast to bool" >> {
          val exp = Transformers.parseTransform("$1::bool")
          exp.eval(Array("", "true")) mustEqual true
        }
        "cast to boolean" >> {
          val exp = Transformers.parseTransform("$1::boolean")
          exp.eval(Array("", "false")) mustEqual false
        }
        "cast to string" >> {
          val exp = Transformers.parseTransform("$1::string")
          exp.eval(Array("", "1")) mustEqual "1"
          exp.eval(Array("", 1)) mustEqual "1"
        }
      }

      "handle dates" >> {
        val testDate = Converters.convert("2015-01-01T00:00:00.000Z", classOf[Date])
        testDate must not(beNull)

        "date with custom format" >> {
          val exp = Transformers.parseTransform("date('yyyyMMdd', $1)")
          exp.eval(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
        }
        "date with a realistic custom format" >> {
          val exp = Transformers.parseTransform("date('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', $1)")
          exp.eval(Array("", "2015-01-01T00:00:00.000000")).asInstanceOf[Date] must be equalTo testDate
        }
        "datetime" >> {
          val exp = Transformers.parseTransform("datetime($1)")
          exp.eval(Array("", "2015-01-01T00:00:00.000Z")).asInstanceOf[Date] must be equalTo testDate
        }
        "dateTime" >> {
          val exp = Transformers.parseTransform("dateTime($1)")
          exp.eval(Array("", "2015-01-01T00:00:00.000Z")).asInstanceOf[Date] must be equalTo testDate
        }
        "isodate" >> {
          val exp = Transformers.parseTransform("isodate($1)")
          exp.eval(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
        }
        "basicDate" >> {
          val exp = Transformers.parseTransform("basicDate($1)")
          exp.eval(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
        }
        "isodatetime" >> {
          val exp = Transformers.parseTransform("isodatetime($1)")
          exp.eval(Array("", "20150101T000000.000Z")).asInstanceOf[Date] must be equalTo testDate
        }
        "basicDateTime" >> {
          val exp = Transformers.parseTransform("basicDateTime($1)")
          exp.eval(Array("", "20150101T000000.000Z")).asInstanceOf[Date] must be equalTo testDate
        }
        "basicDateTimeNoMillis" >> {
          val exp = Transformers.parseTransform("basicDateTimeNoMillis($1)")
          exp.eval(Array("", "20150101T000000Z")).asInstanceOf[Date] must be equalTo testDate
        }
        "dateHourMinuteSecondMillis" >> {
          val exp = Transformers.parseTransform("dateHourMinuteSecondMillis($1)")
          exp.eval(Array("", "2015-01-01T00:00:00.000")).asInstanceOf[Date] must be equalTo testDate
        }
        "millisToDate" >> {
          val millis = testDate.getTime
          val exp = Transformers.parseTransform("millisToDate($1)")
          exp.eval(Array("", millis)).asInstanceOf[Date] must be equalTo testDate
        }

        "secsToDate" >> {
          val secs = testDate.getTime / 1000L
          val exp = Transformers.parseTransform("secsToDate($1)")
          exp.eval(Array("", secs)).asInstanceOf[Date] must be equalTo testDate
        }

        "transform a date to a string" >> {
          val d = LocalDateTime.now()
          val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
          val exp = Transformers.parseTransform("dateToString('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', $1)")
          exp.eval(Array("", Date.from(d.toInstant(ZoneOffset.UTC)))).asInstanceOf[String] must be equalTo fmt.format(d)
        }
        "work with the printf functions" >> {
          val exp = Transformers.parseTransform("datetime(printf('%s-%s-%sT00:00:00.000Z', $1, $2, $3))")
          exp.eval(Array("", "2015", "01", "01")) must be equalTo testDate
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
        res must not(beNull)
        res.getClass mustEqual classOf[Point]
        res.asInstanceOf[Point] mustEqual geoFac.createPoint(new Coordinate(55, 56))
      }

      "handle multipoint wkt and objects" >> {
        val geoFac = new GeometryFactory()
        val multiPoint = geoFac.createMultiPoint(Array(new Coordinate(45.0, 45.0), new Coordinate(50, 52)))
        val trans = Transformers.parseTransform("multipoint($0)")
        trans.eval(Array("Multipoint((45.0 45.0), (50 52))")).asInstanceOf[MultiPoint] mustEqual multiPoint

        // convert objects
        val geom = multiPoint.asInstanceOf[Geometry]
        val res = trans.eval(Array(geom))
        res must not(beNull)
        res.getClass mustEqual classOf[MultiPoint]
        res.asInstanceOf[MultiPoint] mustEqual WKTUtils.read("Multipoint((45.0 45.0), (50 52))")
      }

      "handle linestring wkt and objects" >> {
        val geoFac = new GeometryFactory()
        val lineStr = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        val trans = Transformers.parseTransform("linestring($0)")
        trans.eval(Array("Linestring(102 0, 103 1, 104 0, 105 1)")).asInstanceOf[LineString] mustEqual lineStr

        // type conversion
        val geom = lineStr.asInstanceOf[Geometry]
        val res = trans.eval(Array(geom))
        res must not(beNull)
        res.getClass mustEqual classOf[LineString]
        res.asInstanceOf[LineString] mustEqual WKTUtils.read("Linestring(102 0, 103 1, 104 0, 105 1)")
      }

      "handle multilinestring wkt and objects" >> {
        val geoFac = new GeometryFactory()
        val multiLineStr = geoFac.createMultiLineString(Array(
          geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
            geoFac.createLineString(Seq((0, 0), (1, 2), (2, 3), (4, 5)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        ))
        val trans = Transformers.parseTransform("multilinestring($0)")
        trans.eval(Array("MultiLinestring((102 0, 103 1, 104 0, 105 1), (0 0, 1 2, 2 3, 4 5))")).asInstanceOf[MultiLineString] mustEqual multiLineStr

        // type conversion
        val geom = multiLineStr.asInstanceOf[Geometry]
        val res = trans.eval(Array(geom))
        res must not(beNull)
        res.getClass mustEqual classOf[MultiLineString]
        res.asInstanceOf[MultiLineString] mustEqual WKTUtils.read("MultiLinestring((102 0, 103 1, 104 0, 105 1), (0 0, 1 2, 2 3, 4 5))")
      }

      "handle polygon wkt and objects" >> {
        val geoFac = new GeometryFactory()
        val poly = geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        val trans = Transformers.parseTransform("polygon($0)")
        trans.eval(Array("polygon((100 0, 101 0, 101 1, 100 1, 100 0))")).asInstanceOf[Polygon] mustEqual poly

        // type conversion
        val geom = poly.asInstanceOf[Polygon]
        val res = trans.eval(Array(geom))
        res must not(beNull)
        res.getClass mustEqual classOf[Polygon]
        res.asInstanceOf[Polygon] mustEqual WKTUtils.read("polygon((100 0, 101 0, 101 1, 100 1, 100 0))")
      }

      "handle multipolygon wkt and objects" >> {
        val geoFac = new GeometryFactory()
        val multiPoly = geoFac.createMultiPolygon(Array(
          geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
          geoFac.createPolygon(Seq((10, 0), (11, 0), (11, 1), (10, 1), (10, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        ))
        val trans = Transformers.parseTransform("multipolygon($0)")
        trans.eval(Array("multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), ((10 0, 11 0, 11 1, 10 1, 10 0)))")).asInstanceOf[MultiPolygon] mustEqual multiPoly

        // type conversion
        val geom = multiPoly.asInstanceOf[MultiPolygon]
        val res = trans.eval(Array(geom))
        res must not(beNull)
        res.getClass mustEqual classOf[MultiPolygon]
        res.asInstanceOf[MultiPolygon] mustEqual WKTUtils.read("multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), ((10 0, 11 0, 11 1, 10 1, 10 0)))")
      }

      "handle geometry wkt and objects" >> {
        val geoFac = new GeometryFactory()
        val lineStr = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        val trans = Transformers.parseTransform("geometry($0)")
        trans.eval(Array("Linestring(102 0, 103 1, 104 0, 105 1)")).asInstanceOf[Geometry] must be equalTo lineStr

        // type conversion
        val geom = lineStr.asInstanceOf[Geometry]
        val res = trans.eval(Array(geom))
        res must not(beNull)
        res.asInstanceOf[AnyRef] must beAnInstanceOf[Geometry]
        res.asInstanceOf[Geometry] mustEqual WKTUtils.read("Linestring(102 0, 103 1, 104 0, 105 1)\"")
      }

      "handle geometrycollection wkt and objects" >> {
        val geoFac = new GeometryFactory()
        val geoCol = geoFac.createGeometryCollection(Array(
          geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
          geoFac.createMultiPolygon(Array(
            geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
            geoFac.createPolygon(Seq((10, 0), (11, 0), (11, 1), (10, 1), (10, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
          ))
        ))
        val trans = Transformers.parseTransform("geometrycollection($0)")
        trans.eval(Array("GeometryCollection(Linestring(102 0, 103 1, 104 0, 105 1)," +
          "multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), " +
          "((10 0, 11 0, 11 1, 10 1, 10 0))))")).asInstanceOf[GeometryCollection] mustEqual geoCol

        // type conversion
        val geom = geoCol.asInstanceOf[Geometry]
        val res = trans.eval(Array(geom))
        res must not(beNull)
        res.getClass mustEqual classOf[GeometryCollection]
        res.asInstanceOf[GeometryCollection] mustEqual WKTUtils.read(
          "GeometryCollection(Linestring(102 0, 103 1, 104 0, 105 1), " +
          "multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), ((10 0, 11 0, 11 1, 10 1, 10 0))))")
      }

      "reproject to EPSG 4326" >> {
        val geom = WKTUtils.read("POINT (1113194.91 1689200.14)")
        val trans = Transformers.parseTransform("projectFrom('EPSG:3857',$1)")
        val transformed = trans.eval(Array("", geom))
        transformed must not(beNull)
        transformed.getClass mustEqual classOf[Point]
        transformed.asInstanceOf[Point].getX must beCloseTo(10d, 0.001)
        transformed.asInstanceOf[Point].getY must beCloseTo(15d, 0.001)
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
          val res = exp.eval(Array(null))
          res must not(beNull)
          res.getClass mustEqual classOf[String]
        }
        "z3 uuid" >> {
          val exp = Transformers.parseTransform("uuidZ3($0, $1, 'week')")
          val geom = WKTUtils.read("POINT (103 1)")
          val date = Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])
          val res = exp.eval(Array(geom, date))
          res must not(beNull)
          res.getClass mustEqual classOf[String]
        }
        "z3 centroid uuid" >> {
          val exp = Transformers.parseTransform("uuidZ3Centroid($0, $1, 'week')")
          val geom = WKTUtils.read("LINESTRING (102 0, 103 1, 104 0, 105 1)")
          val date = Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])
          val res = exp.eval(Array(geom, date))
          res must not(beNull)
          res.getClass mustEqual classOf[String]
        }
        "base64" >> {
          val exp = Transformers.parseTransform("base64($0)")
          exp.eval(Array(bytes)) must be equalTo Base64.encodeBase64URLSafeString(bytes)
        }
      }

      "handle whitespace in functions" >> {
        val variants = Seq(
          "printf('%s-%s-%sT00:00:00.000Z', $1, $2, $3)",
          "printf ( '%s-%s-%sT00:00:00.000Z' , $1 , $2 , $3 )"
        )
        foreach(variants) { t =>
          val exp = Transformers.parseTransform(t)
          exp.eval(Array("", "2015", "01", "01")) mustEqual "2015-01-01T00:00:00.000Z"
        }
      }

      "handle named values" >> {
        val ctx = EvaluationContext(IndexedSeq(null, "foo", null), Array[Any](null, "bar", null), null, Map.empty)
        val exp = Transformers.parseTransform("capitalize($foo)")
        exp.eval(Array(null))(ctx) must be equalTo "Bar"
      }

      "handle exceptions to casting" >> {
        val exp = Transformers.parseTransform("try($1::int, 0)")
        exp.eval(Array("", "1")).asInstanceOf[Int] mustEqual 1
        exp.eval(Array("", "")).asInstanceOf[Int] mustEqual 0
        exp.eval(Array("", "abcd")).asInstanceOf[Int] mustEqual 0
      }

      "handle exceptions to millisecond conversions" >> {
        val exp = Transformers.parseTransform("try(millisToDate($1), now())")
        val millis = 100000L
        exp.eval(Array("", millis)).asInstanceOf[Date] mustEqual new Date(millis)
        exp.eval(Array("", "")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
        exp.eval(Array("", "abcd")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
      }

      "handle exceptions to millisecond conversions with null defaults" >> {
        val exp = Transformers.parseTransform("try(millisToDate($1), null)")
        val millis = 100000L
        exp.eval(Array("", millis)).asInstanceOf[Date] mustEqual new Date(millis)
        exp.eval(Array("", "")).asInstanceOf[Date] must beNull
        exp.eval(Array("", "abcd")).asInstanceOf[Date] must beNull
      }

      "handle exceptions to second conversions" >> {
        val exp = Transformers.parseTransform("try(secsToDate($1), now())")
        val secs = 100L
        exp.eval(Array("", secs)).asInstanceOf[Date] mustEqual new Date(secs*1000L)
        exp.eval(Array("", "")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 1000)
        exp.eval(Array("", "abcd")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
      }

      "handle exceptions to second conversions with null defaults" >> {
        val exp = Transformers.parseTransform("try(secsToDate($1), null)")
        val secs = 100L
        exp.eval(Array("", secs)).asInstanceOf[Date] mustEqual new Date(secs*1000L)
        exp.eval(Array("", "")).asInstanceOf[Date] must beNull
        exp.eval(Array("", "abcd")).asInstanceOf[Date] must beNull
      }

      "allow spaces in try statements" >> {
        foreach(Seq("try($1::int,0)", "try ( $1::int, 0 )")) { t =>
          val exp = Transformers.parseTransform(t)
          exp.eval(Array("", "1")).asInstanceOf[Int] mustEqual 1
          exp.eval(Array("", "")).asInstanceOf[Int] mustEqual 0
          exp.eval(Array("", "abcd")).asInstanceOf[Int] mustEqual 0
        }
      }
    }

    "handle math" >> {
      "add" >> {
        val exp1 = Transformers.parseTransform("add($1,$2)")
        exp1.eval(Array("","1","2")) mustEqual 3.0
        exp1.eval(Array("","-1","2")) mustEqual 1.0

        val exp2 = Transformers.parseTransform("add($1,$2,$3)")
        exp2.eval(Array("","1","2","3.0")) mustEqual 6.0
        exp2.eval(Array("","-1","2","3.0")) mustEqual 4.0
      }

      "multiply" >> {
        val exp1 = Transformers.parseTransform("multiply($1,$2)")
        exp1.eval(Array("","1","2")) mustEqual 2.0
        exp1.eval(Array("","-1","2")) mustEqual -2.0

        val exp2 = Transformers.parseTransform("multiply($1,$2,$3)")
        exp2.eval(Array("","1","2","3.0")) mustEqual 6.0
        exp2.eval(Array("","-1","2","3.0")) mustEqual -6.0
      }

      "subtract" >> {
        val exp1 = Transformers.parseTransform("subtract($1,$2)")
        exp1.eval(Array("","2","1")) mustEqual 1.0
        exp1.eval(Array("","-1","2")) mustEqual -3.0

        val exp2 = Transformers.parseTransform("subtract($1,$2,$3)")
        exp2.eval(Array("","1","2","3.0")) mustEqual -4.0
        exp2.eval(Array("","-1","2","3.0")) mustEqual -6.0
      }

      "divide" >> {
        val exp1 = Transformers.parseTransform("divide($1,$2)")
        exp1.eval(Array("","2","1")) mustEqual 2.0
        exp1.eval(Array("","-1","2")) mustEqual -0.5

        val exp2 = Transformers.parseTransform("divide($1,$2,$3)")
        exp2.eval(Array("","1","2","3.0")) mustEqual (1.0/2/3) // 0.166666666666
        exp2.eval(Array("","-1","2","3.0")) mustEqual (-1.0/2/3) // -0.166666666666
      }
      "mean" >> {
        val exp1 = Transformers.parseTransform("mean($1,$2,$3,$4)")
        exp1.eval(Array("","1","2","3","4")) mustEqual 2.5
      }
      "min" >> {
        val exp1 = Transformers.parseTransform("min($1,$2,$3,$4)::int")
        exp1.eval(Array("","1","2","3","4")) mustEqual 1
      }
      "max" >> {
        val exp1 = Transformers.parseTransform("max($1,$2,$3,$4)::int")
        exp1.eval(Array("","1","2","3","4")) mustEqual 4
      }
      "allow for number formatting using printf" >> {
        val exp = Transformers.parseTransform("printf('%.2f', divide($1,$2,$3))")
        exp.eval(Array("","-1","2","3.0")) mustEqual "-0.17"
      }
      "allow for number formatting using printf" >> {
        val exp = Transformers.parseTransform("printf('%.2f', divide(-1, 2, 3))")
        exp.eval(Array()) mustEqual "-0.17"
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
        "integer equals" >> {
          val exp = Transformers.parsePred("integerEq($1::int, $2::int)")
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

      "stringTo functions" >> {
        "stringToDouble" >> {
          "double stringToDouble zero default" >> {
            val exp = Transformers.parseTransform("stringToDouble($1, 0.0)")
            exp.eval(Array("", "1.2")) mustEqual 1.2
            exp.eval(Array("", "")) mustEqual 0.0
            exp.eval(Array("", null)) mustEqual 0.0
            exp.eval(Array("", "notadouble")) mustEqual 0.0
          }
          "double stringToDouble null default" >> {
            val exp = Transformers.parseTransform("stringToDouble($1, $2)")
            exp.eval(Array("", "1.2", null)) mustEqual 1.2
            exp.eval(Array("", "", null)) mustEqual null
            exp.eval(Array("", null, null)) mustEqual null
            exp.eval(Array("", "notadouble", null)) mustEqual null
          }
        }
        "stringToInt" >> {
          "int stringToInt zero default" >> {
            val exp = Transformers.parseTransform("stringToInt($1, 0)")
            exp.eval(Array("", "2")) mustEqual 2
            exp.eval(Array("", "")) mustEqual 0
            exp.eval(Array("", null)) mustEqual 0
            exp.eval(Array("", "1.2")) mustEqual 0
          }
          "int stringToInt null default" >> {
            val exp = Transformers.parseTransform("stringToInt($1, $2)")
            exp.eval(Array("", "2", null)) mustEqual 2
            exp.eval(Array("", "", null)) mustEqual null
            exp.eval(Array("", null, null)) mustEqual null
            exp.eval(Array("", "1.2", null)) mustEqual null
          }
        }
        "stringToInteger" >> {
          "int stringToInteger zero default" >> {
            val exp = Transformers.parseTransform("stringToInteger($1, 0)")
            exp.eval(Array("", "2")) mustEqual 2
            exp.eval(Array("", "")) mustEqual 0
            exp.eval(Array("", null)) mustEqual 0
            exp.eval(Array("", "1.2")) mustEqual 0
          }
          "int stringToInteger null default" >> {
            val exp = Transformers.parseTransform("stringToInteger($1, $2)")
            exp.eval(Array("", "2", null)) mustEqual 2
            exp.eval(Array("", "", null)) mustEqual null
            exp.eval(Array("", null, null)) mustEqual null
            exp.eval(Array("", "1.2", null)) mustEqual null
          }
        }
        "stringToLong" >> {
          "long stringToLong zero default" >> {
            val exp = Transformers.parseTransform("stringToLong($1, 0L)")
            exp.eval(Array("", "22960000000")) mustEqual 22960000000L
            exp.eval(Array("", "")) mustEqual 0L
            exp.eval(Array("", null)) mustEqual 0L
            exp.eval(Array("", "1.2")) mustEqual 0L
          }
          "long stringToLong null default" >> {
            val exp = Transformers.parseTransform("stringToLong($1, $2)")
            exp.eval(Array("", "22960000000", null)) mustEqual 22960000000L
            exp.eval(Array("", "", null)) mustEqual null
            exp.eval(Array("", null, null)) mustEqual null
            exp.eval(Array("", "1.2", null)) mustEqual null
          }
        }
        "stringToFloat" >> {
          "float stringToFloat zero default" >> {
            val exp = Transformers.parseTransform("stringToFloat($1, 0.0f)")
            exp.eval(Array("", "1.2")) mustEqual 1.2f
            exp.eval(Array("", "")) mustEqual 0.0f
            exp.eval(Array("", null)) mustEqual 0.0f
            exp.eval(Array("", "notafloat")) mustEqual 0.0f
          }
          "float stringToFloat zero default" >> {
            val exp = Transformers.parseTransform("stringToFloat($1, $2)")
            exp.eval(Array("", "1.2", null)) mustEqual 1.2f
            exp.eval(Array("", "", null)) mustEqual null
            exp.eval(Array("", null, null)) mustEqual null
            exp.eval(Array("", "notafloat", null)) mustEqual null
          }
        }
        "stringToBoolean" >> {
          "boolean stringToBoolean false default" >> {
            val exp = Transformers.parseTransform("stringToBoolean($1, false)")
            exp.eval(Array("", "true")) mustEqual true
            exp.eval(Array("", "")) mustEqual false
            exp.eval(Array("", null)) mustEqual false
            exp.eval(Array("", "18")) mustEqual false
          }
          "boolean stringToBoolean null default" >> {
            val exp = Transformers.parseTransform("stringToBoolean($1,$2)")
            exp.eval(Array("", "true", null)) mustEqual true
            exp.eval(Array("", "", null)) mustEqual null
            exp.eval(Array("", null, null)) mustEqual null
            exp.eval(Array("", "18", null)) mustEqual null
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

      "support cql functions" >> {
        "buffer" >> {
          val exp = Transformers.parseTransform("cql:buffer($1, $2)")
          val buf = exp.eval(Array(null, "POINT(1 1)", 2.0))
          buf must not(beNull)
          buf.getClass mustEqual classOf[Polygon]
          buf.asInstanceOf[Polygon].getCentroid.getX must beCloseTo(1, 0.0001)
          buf.asInstanceOf[Polygon].getCentroid.getY must beCloseTo(1, 0.0001)
          // note: area is not particularly close as there aren't very many points in the polygon
          buf.asInstanceOf[Polygon].getArea must beCloseTo(math.Pi * 4.0, 0.2)
        }
      }
    }

    "return null for non-existing fields" >> {
      val fieldsCtx = new EvaluationContextImpl(IndexedSeq("foo", "bar"), Array("5", "10"), null, Map.empty)
      Transformers.parseTransform("$b").eval(Array())(fieldsCtx) mustEqual null
      Transformers.parseTransform("$bar").eval(Array())(fieldsCtx) mustEqual "10"
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

    "parse maps" >> {
      "default delimiter" >> {
        val trans = Transformers.parseTransform("parseMap('String->Int', $0)")
        val res = trans.eval(Array("a->1,b->2,c->3"))
        res must not(beNull)
        res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.Map[String, Int]]
        res.asInstanceOf[java.util.Map[String, Int]].size mustEqual 3
        res.asInstanceOf[java.util.Map[String, Int]].toMap mustEqual Map("a" -> 1, "b" -> 2, "c" -> 3)
      }
      "custom delimiter" >> {
        val trans = Transformers.parseTransform("parseMap('String->Int', $0, '%', ';')")
        val res = trans.eval(Array("a%1;b%2;c%3"))
        res must not(beNull)
        res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.Map[String, Int]]
        res.asInstanceOf[java.util.Map[String, Int]].size mustEqual 3
        res.asInstanceOf[java.util.Map[String, Int]].toMap mustEqual Map("a" -> 1, "b" -> 2, "c" -> 3)
      }
      "throw exception for invalid values" >> {
        val trans = Transformers.parseTransform("parseMap('String->Int', $0)")
        trans.eval(Array("a->1,b->2,c->d")) must throwAn[IllegalArgumentException]
      }
    }

    "handle default values" >> {
      val trans = Transformers.parseTransform("withDefault($0, 'foo')")
      trans.eval(Array(null)) mustEqual "foo"
      trans.eval(Array("bar")) mustEqual "bar"
      trans.eval(Array("")) mustEqual ""
    }

    "strip strings" >> {

      "strip quotes" >> {
        val trans = Transformers.parseTransform("stripQuotes($0)")
        trans.eval(Array("'foo'")) mustEqual "foo"
        trans.eval(Array("\"foo'")) mustEqual "foo"
        trans.eval(Array("\"'foo'")) mustEqual "foo"

        // white space is preserved
        trans.eval(Array("'foo\t\t")) mustEqual "foo\t\t"
        trans.eval(Array("  foo'\"\"")) mustEqual "  foo"
      }

      "all whitespace no args" >> {
        val trans = Transformers.parseTransform("strip($0)")
        trans.eval(Array("\t   foo   \t\t\t")) mustEqual "foo"
      }

      "from start and front with strip" >> {
        val trans = Transformers.parseTransform("strip($0, '\\'')")
        trans.eval(Array("'foo'")) mustEqual "foo"
        trans.eval(Array("'foo")) mustEqual "foo"
        trans.eval(Array("foo'")) mustEqual "foo"
      }

      "multiple chars (e.g. quotes)" >> {
        val trans = Transformers.parseTransform("strip($0, '\\'\\\"')")
        trans.eval(Array("'foo'")) mustEqual "foo"
        trans.eval(Array("\"foo'")) mustEqual "foo"
        trans.eval(Array("\"'foo'")) mustEqual "foo"
        trans.eval(Array("'foo")) mustEqual "foo"
        trans.eval(Array("foo'\"\"")) mustEqual "foo"
      }

      "prefix only" >> {
        val trans = Transformers.parseTransform("stripPrefix($0, '\\'')")
        trans.eval(Array("'foo'", "'")) mustEqual "foo'"
        trans.eval(Array("'foo", "'")) mustEqual "foo"
        trans.eval(Array("foo'", "'")) mustEqual "foo'"
        trans.eval(Array(" 'foo'", "'")) mustEqual " 'foo'"
      }

      "prefix with  multiple chars" >> {
        val trans = Transformers.parseTransform("stripPrefix($0, '\\'\\\"')")
        trans.eval(Array("'foo'", "'")) mustEqual "foo'"
        trans.eval(Array("'foo", "'")) mustEqual "foo"
        trans.eval(Array("foo'", "'")) mustEqual "foo'"
        trans.eval(Array("\"'foo\"", "'")) mustEqual "foo\""
        trans.eval(Array("\"\"\"'foo'", "'")) mustEqual "foo'"
      }

      "suffix only" >> {
        val trans = Transformers.parseTransform("stripSuffix($0, '\\'')")
        trans.eval(Array("'foo'")) mustEqual "'foo"
        trans.eval(Array("'foo")) mustEqual "'foo"
        trans.eval(Array("foo'")) mustEqual "foo"
      }

      "suffix with preserving whitespace" >> {
        val trans = Transformers.parseTransform("stripSuffix($0, 'ab')")
        trans.eval(Array("fooab ")) mustEqual "fooab "
      }

      "suffix multiple chars" >> {
        val trans = Transformers.parseTransform("stripSuffix($0, '\\'\\\"')")
        trans.eval(Array("'\"foo'")) mustEqual "'\"foo"
        trans.eval(Array("'\"foo")) mustEqual "'\"foo"
        trans.eval(Array("\"foo'")) mustEqual "\"foo"
        trans.eval(Array("'foo\"'")) mustEqual "'foo"
        trans.eval(Array("'foo\"")) mustEqual "'foo"
        trans.eval(Array("foo'\"")) mustEqual "foo"
      }

      "something other than quotes" >> {
        val trans = Transformers.parseTransform("strip($0, 'X')")
        trans.eval(Array("XfooX")) mustEqual "foo"
        trans.eval(Array("Xfoo")) mustEqual "foo"
        trans.eval(Array("fooX")) mustEqual "foo"
      }
    }

    "remove strings" >> {
      val trans = Transformers.parseTransform("remove($0, '\\'')")
      trans.eval(Array("'foo'")) mustEqual "foo"
      trans.eval(Array("'foo")) mustEqual "foo"
      trans.eval(Array("foo'")) mustEqual "foo"
      trans.eval(Array("f'o'o'")) mustEqual "foo"

      Transformers.parseTransform("remove($0, 'abc')").eval(Array("foabco")) mustEqual "foo"
    }

    "replace" >> {
      val trans = Transformers.parseTransform("replace($0, '\\'', '\\\"')")
      trans.eval(Array("'foo'")) mustEqual "\"foo\""
      trans.eval(Array("'foo")) mustEqual "\"foo"
      trans.eval(Array("foo'")) mustEqual "foo\""
      trans.eval(Array("f'o'o'")) mustEqual "f\"o\"o\""

      Transformers.parseTransform("replace($0, 'a', 'o')").eval(Array("faa")) mustEqual "foo"
    }
  }
}
