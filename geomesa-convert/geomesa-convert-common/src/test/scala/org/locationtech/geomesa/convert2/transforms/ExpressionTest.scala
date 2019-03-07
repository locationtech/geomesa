/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Date

import com.google.common.hash.Hashing
import org.apache.commons.codec.binary.Base64
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.{EvaluationContext, EvaluationContextImpl}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class ExpressionTest extends Specification {

  import scala.collection.JavaConverters._

  implicit val ctx: EvaluationContext = EvaluationContext.empty

  val testDate = Converters.convert("2015-01-01T00:00:00.000Z", classOf[Date])

  val testBytes = Array.ofDim[Byte](32)
  new Random(-9L).nextBytes(testBytes)

  "Transformers" should {

    "allow literal strings" >> {
      val exp = Expression("'hello'")
      exp.eval(Array(null)) must be equalTo "hello"
    }
    "allow quoted strings" >> {
      val exp = Expression("'he\\'llo'")
      exp.eval(Array(null)) must be equalTo "he'llo"
    }
    "allow empty literal strings" >> {
      val exp = Expression("''")
      exp.eval(Array(null)) must be equalTo ""
    }
    "allow native ints" >> {
      val res = Expression("1").eval(Array(null))
      res must not(beNull)
      res.getClass mustEqual classOf[java.lang.Integer]
      res mustEqual 1
    }
    "allow native longs" >> {
      val res = Expression("1L").eval(Array(null))
      res must not(beNull)
      res.getClass mustEqual classOf[java.lang.Long]
      res mustEqual 1L
    }
    "allow native floats" >> {
      val tests = Seq(("1.0", 1f), ("1.0", 1f), (".1", .1f), ("0.1", .1f), ("-1.0", -1f))
      foreach(tests) { case (s, expected) =>
        foreach(Seq("f", "F")) { suffix =>
          val res = Expression(s + suffix).eval(Array(null))
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
          val res = Expression(s + suffix).eval(Array(null))
          res must not(beNull)
          res.getClass mustEqual classOf[java.lang.Double]
          res mustEqual expected
        }
      }
    }
    "allow native booleans" >> {
      Expression("false").eval(Array(null)) mustEqual false
      Expression("true").eval(Array(null)) mustEqual true
    }
    "allow native nulls" >> {
      Expression("null").eval(Array(null)) must beNull
    }
    "trim" >> {
      val exp = Expression("trim($1)")
      exp.eval(Array("", "foo ", "bar")) must be equalTo "foo"
    }
    "capitalize" >> {
      val exp = Expression("capitalize($1)")
      exp.eval(Array("", "foo", "bar")) must be equalTo "Foo"
    }
    "lowercase" >> {
      val exp = Expression("lowercase($1)")
      exp.eval(Array("", "FOO", "bar")) must be equalTo "foo"
    }
    "uppercase" >> {
      val exp = Expression("uppercase($1)")
      exp.eval(Array("", "FoO")) must be equalTo "FOO"
    }
    "regexReplace" >> {
      val exp = Expression("regexReplace('foo'::r,'bar',$1)")
      exp.eval(Array("", "foobar")) must be equalTo "barbar"
    }
    "compound expressions" >> {
      val exp = Expression("regexReplace('foo'::r,'bar',trim($1))")
      exp.eval(Array("", " foobar ")) must be equalTo "barbar"
    }
    "take substrings" >> {
      foreach(Seq("substring", "substr")) { fn =>
        val exp = Expression(s"$fn($$1, 2, 5)")
        exp.eval(Array("", "foobarbaz")) must be equalTo "foobarbaz".substring(2, 5)
      }
    }
    "calculate strlen" >> {
      val exp = Expression("strlen($1)")
      exp.eval(Array("", "FOO")) must be equalTo 3
    }
    "calculate length" >> {
      val exp = Expression("length($1)")
      exp.eval(Array("", "FOO")) must be equalTo 3
    }
    "convert toString" >> {
      val exp = Expression("toString($1)")
      exp.eval(Array("", 5)) must be equalTo "5"
    }
    "concat with toString" >> {
      val exp = Expression("concat(toString($1), toString($2))")
      exp.eval(Array("", 5, 6)) must be equalTo "56"
    }
    "concat many args" >> {
      val exp = Expression("concat($1, $2, $3, $4, $5, $6)")
      exp.eval(Array("", 1, 2, 3, 4, 5, 6)) must be equalTo "123456"
    }
    "mkstring" >> {
      val exp = Expression("mkstring(',', $1, $2, $3, $4, $5, $6)")
      exp.eval(Array("", 1, 2, 3, 4, 5, 6)) must be equalTo "1,2,3,4,5,6"
    }
    "convert emptyToNull" >> {
      val exp = Expression("emptyToNull($1)")
      exp.eval(Array("", "foo")) mustEqual "foo"
      exp.eval(Array("", "")) must beNull
      exp.eval(Array("", "  ")) must beNull
      exp.eval(Array("", null)) must beNull
    }
    "printf" >> {
      val exp = Expression("printf('%s-%s-%sT00:00:00.000Z', '2015', '01', '01')")
      exp.eval(Array()) mustEqual "2015-01-01T00:00:00.000Z"
    }
    "handle non string ints" >> {
      val exp = Expression("$2")
      exp.eval(Array("", "1", 2)) must be equalTo 2
    }
    "cast to int" >> {
      foreach(Seq("int", "integer")) { cast =>
        val exp = Expression(s"$$1::$cast")
        exp.eval(Array("", "1")) mustEqual 1
        exp.eval(Array("", 1E2)) mustEqual 100
        exp.eval(Array("", 1)) mustEqual 1
        exp.eval(Array("", 1D)) mustEqual 1
        exp.eval(Array("", 1F)) mustEqual 1
        exp.eval(Array("", 1L)) mustEqual 1
      }
    }
    "cast to long" >> {
      val exp = Expression("$1::long")
      exp.eval(Array("", "1")) mustEqual 1L
      exp.eval(Array("", 1E2)) mustEqual 100L
      exp.eval(Array("", 1)) mustEqual 1L
      exp.eval(Array("", 1D)) mustEqual 1L
      exp.eval(Array("", 1F)) mustEqual 1L
      exp.eval(Array("", 1L)) mustEqual 1L
    }
    "cast to float" >> {
      val exp = Expression("$1::float")
      exp.eval(Array("", "1")) mustEqual 1F
      exp.eval(Array("", 1E2)) mustEqual 100F
      exp.eval(Array("", 1)) mustEqual 1F
      exp.eval(Array("", 1D)) mustEqual 1F
      exp.eval(Array("", 1F)) mustEqual 1F
      exp.eval(Array("", 1L)) mustEqual 1F
    }
    "cast to double" >> {
      val exp = Expression("$1::double")
      exp.eval(Array("", "1")) mustEqual 1D
      exp.eval(Array("", 1E2)) mustEqual 100D
      exp.eval(Array("", 1)) mustEqual 1D
      exp.eval(Array("", 1D)) mustEqual 1D
      exp.eval(Array("", 1F)) mustEqual 1D
      exp.eval(Array("", 1L)) mustEqual 1D
    }
    "cast to boolean" >> {
      foreach(Seq("bool", "boolean")) { cast =>
        val exp = Expression(s"$$1::$cast")
        exp.eval(Array("", "true")) mustEqual true
        exp.eval(Array("", "false")) mustEqual false
      }
    }
    "cast to string" >> {
      val exp = Expression("$1::string")
      exp.eval(Array("", "1")) mustEqual "1"
      exp.eval(Array("", 1)) mustEqual "1"
    }
    "parse dates with custom format" >> {
      val exp = Expression("date('yyyyMMdd', $1)")
      exp.eval(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse dates with a realistic custom format" >> {
      val exp = Expression("date('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', $1)")
      exp.eval(Array("", "2015-01-01T00:00:00.000000")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse datetime" >> {
      foreach(Seq("datetime", "dateTime")) { cast =>
        val exp = Expression(s"$cast($$1)")
        exp.eval(Array("", "2015-01-01T00:00:00.000Z")).asInstanceOf[Date] must be equalTo testDate
      }
    }
    "parse isodate" >> {
      val exp = Expression("isodate($1)")
      exp.eval(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse basicDate" >> {
      val exp = Expression("basicDate($1)")
      exp.eval(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse isodatetime" >> {
      val exp = Expression("isodatetime($1)")
      exp.eval(Array("", "20150101T000000.000Z")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse basicDateTime" >> {
      val exp = Expression("basicDateTime($1)")
      exp.eval(Array("", "20150101T000000.000Z")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse basicDateTimeNoMillis" >> {
      val exp = Expression("basicDateTimeNoMillis($1)")
      exp.eval(Array("", "20150101T000000Z")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse dateHourMinuteSecondMillis" >> {
      val exp = Expression("dateHourMinuteSecondMillis($1)")
      exp.eval(Array("", "2015-01-01T00:00:00.000")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse millisToDate" >> {
      val millis = testDate.getTime
      val exp = Expression("millisToDate($1)")
      exp.eval(Array("", millis)).asInstanceOf[Date] must be equalTo testDate
    }
    "parse secsToDate" >> {
      val secs = testDate.getTime / 1000L
      val exp = Expression("secsToDate($1)")
      exp.eval(Array("", secs)).asInstanceOf[Date] must be equalTo testDate
    }
    "transform a date to a string" >> {
      val d = LocalDateTime.now()
      val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
      val exp = Expression("dateToString('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', $1)")
      exp.eval(Array("", Date.from(d.toInstant(ZoneOffset.UTC)))).asInstanceOf[String] must be equalTo fmt.format(d)
    }
    "parse date strings from printf" >> {
      val exp = Expression("datetime(printf('%s-%s-%sT00:00:00.000Z', $1, $2, $3))")
      exp.eval(Array("", "2015", "01", "01")) must be equalTo testDate
    }
    "parse point geometries" >> {
      val exp = Expression("point($1, $2)")
      exp.eval(Array("", 45.0, 45.0)).asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(45.0, 45.0)

      val trans = Expression("point($0)")
      trans.eval(Array("POINT(50 52)")).asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(50, 52)

      // turn "Geometry" into "Point"
      val geoFac = new GeometryFactory()
      val geom = geoFac.createPoint(new Coordinate(55, 56)).asInstanceOf[Geometry]
      val res = trans.eval(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[Point]
      res.asInstanceOf[Point] mustEqual geoFac.createPoint(new Coordinate(55, 56))
    }
    "parse multipoint wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val multiPoint = geoFac.createMultiPointFromCoords(Array(new Coordinate(45.0, 45.0), new Coordinate(50, 52)))
      val trans = Expression("multipoint($0)")
      trans.eval(Array("Multipoint((45.0 45.0), (50 52))")).asInstanceOf[MultiPoint] mustEqual multiPoint

      // convert objects
      val geom = multiPoint.asInstanceOf[Geometry]
      val res = trans.eval(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[MultiPoint]
      res.asInstanceOf[MultiPoint] mustEqual WKTUtils.read("Multipoint((45.0 45.0), (50 52))")
    }
    "parse linestring wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val lineStr = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      val trans = Expression("linestring($0)")
      trans.eval(Array("Linestring(102 0, 103 1, 104 0, 105 1)")).asInstanceOf[LineString] mustEqual lineStr

      // type conversion
      val geom = lineStr.asInstanceOf[Geometry]
      val res = trans.eval(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[LineString]
      res.asInstanceOf[LineString] mustEqual WKTUtils.read("Linestring(102 0, 103 1, 104 0, 105 1)")
    }
    "parse multilinestring wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val multiLineStr = geoFac.createMultiLineString(Array(
        geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
          geoFac.createLineString(Seq((0, 0), (1, 2), (2, 3), (4, 5)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      ))
      val trans = Expression("multilinestring($0)")
      trans.eval(Array("MultiLinestring((102 0, 103 1, 104 0, 105 1), (0 0, 1 2, 2 3, 4 5))")).asInstanceOf[MultiLineString] mustEqual multiLineStr

      // type conversion
      val geom = multiLineStr.asInstanceOf[Geometry]
      val res = trans.eval(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[MultiLineString]
      res.asInstanceOf[MultiLineString] mustEqual WKTUtils.read("MultiLinestring((102 0, 103 1, 104 0, 105 1), (0 0, 1 2, 2 3, 4 5))")
    }
    "parse polygon wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val poly = geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      val trans = Expression("polygon($0)")
      trans.eval(Array("polygon((100 0, 101 0, 101 1, 100 1, 100 0))")).asInstanceOf[Polygon] mustEqual poly

      // type conversion
      val geom = poly.asInstanceOf[Polygon]
      val res = trans.eval(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[Polygon]
      res.asInstanceOf[Polygon] mustEqual WKTUtils.read("polygon((100 0, 101 0, 101 1, 100 1, 100 0))")
    }
    "parse multipolygon wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val multiPoly = geoFac.createMultiPolygon(Array(
        geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
        geoFac.createPolygon(Seq((10, 0), (11, 0), (11, 1), (10, 1), (10, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      ))
      val trans = Expression("multipolygon($0)")
      trans.eval(Array("multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), ((10 0, 11 0, 11 1, 10 1, 10 0)))")).asInstanceOf[MultiPolygon] mustEqual multiPoly

      // type conversion
      val geom = multiPoly.asInstanceOf[MultiPolygon]
      val res = trans.eval(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[MultiPolygon]
      res.asInstanceOf[MultiPolygon] mustEqual WKTUtils.read("multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), ((10 0, 11 0, 11 1, 10 1, 10 0)))")
    }
    "parse geometry wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val lineStr = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      val trans = Expression("geometry($0)")
      trans.eval(Array("Linestring(102 0, 103 1, 104 0, 105 1)")).asInstanceOf[Geometry] must be equalTo lineStr

      // type conversion
      val geom = lineStr.asInstanceOf[Geometry]
      val res = trans.eval(Array(geom))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[Geometry]
      res.asInstanceOf[Geometry] mustEqual WKTUtils.read("Linestring(102 0, 103 1, 104 0, 105 1)\"")
    }
    "parse geometrycollection wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val geoCol = geoFac.createGeometryCollection(Array(
        geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
        geoFac.createMultiPolygon(Array(
          geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
          geoFac.createPolygon(Seq((10, 0), (11, 0), (11, 1), (10, 1), (10, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        ))
      ))
      val trans = Expression("geometrycollection($0)")
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
      val trans = Expression("projectFrom('EPSG:3857',$1)")
      val transformed = trans.eval(Array("", geom))
      transformed must not(beNull)
      transformed.getClass mustEqual classOf[Point]
      transformed.asInstanceOf[Point].getX must beCloseTo(10d, 0.001)
      transformed.asInstanceOf[Point].getY must beCloseTo(15d, 0.001)
    }
    "generate md5 hashes" >> {
      val hasher = Hashing.md5().newHasher()
      val exp = Expression("md5($0)")
      val hashedResult = exp.eval(Array(testBytes)).asInstanceOf[String]
      hashedResult must be equalTo hasher.putBytes(testBytes).hash().toString
    }
    "generate uuids" >> {
      val exp = Expression("uuid()")
      val res = exp.eval(Array(null))
      res must not(beNull)
      res.getClass mustEqual classOf[String]
    }
    "generate z3 uuids" >> {
      val exp = Expression("uuidZ3($0, $1, 'week')")
      val geom = WKTUtils.read("POINT (103 1)")
      val date = Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])
      val res = exp.eval(Array(geom, date))
      res must not(beNull)
      res.getClass mustEqual classOf[String]
    }
    "generate z3 centroid uuids" >> {
      val exp = Expression("uuidZ3Centroid($0, $1, 'week')")
      val geom = WKTUtils.read("LINESTRING (102 0, 103 1, 104 0, 105 1)")
      val date = Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])
      val res = exp.eval(Array(geom, date))
      res must not(beNull)
      res.getClass mustEqual classOf[String]
    }
    "encode bytes as base64 strings" >> {
      val exp = Expression("base64($0)")
      exp.eval(Array(testBytes)) must be equalTo Base64.encodeBase64URLSafeString(testBytes)
    }
    "handle whitespace in functions" >> {
      val variants = Seq(
        "printf('%s-%s-%sT00:00:00.000Z', $1, $2, $3)",
        "printf ( '%s-%s-%sT00:00:00.000Z' , $1 , $2 , $3 )"
      )
      foreach(variants) { t =>
        val exp = Expression(t)
        exp.eval(Array("", "2015", "01", "01")) mustEqual "2015-01-01T00:00:00.000Z"
      }
    }
    "handle named values" >> {
      val ctx = EvaluationContext(IndexedSeq(null, "foo", null), Array[Any](null, "bar", null), null, Map.empty)
      val exp = Expression("capitalize($foo)")
      exp.eval(Array(null))(ctx) must be equalTo "Bar"
    }
    "handle exceptions to casting" >> {
      val exp = Expression("try($1::int, 0)")
      exp.eval(Array("", "1")).asInstanceOf[Int] mustEqual 1
      exp.eval(Array("", "")).asInstanceOf[Int] mustEqual 0
      exp.eval(Array("", "abcd")).asInstanceOf[Int] mustEqual 0
    }
    "handle exceptions to millisecond conversions" >> {
      val exp = Expression("try(millisToDate($1), now())")
      val millis = 100000L
      exp.eval(Array("", millis)).asInstanceOf[Date] mustEqual new Date(millis)
      exp.eval(Array("", "")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
      exp.eval(Array("", "abcd")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
    }
    "handle exceptions to millisecond conversions with null defaults" >> {
      val exp = Expression("try(millisToDate($1), null)")
      val millis = 100000L
      exp.eval(Array("", millis)).asInstanceOf[Date] mustEqual new Date(millis)
      exp.eval(Array("", "")).asInstanceOf[Date] must beNull
      exp.eval(Array("", "abcd")).asInstanceOf[Date] must beNull
    }
    "handle exceptions to second conversions" >> {
      val exp = Expression("try(secsToDate($1), now())")
      val secs = 100L
      exp.eval(Array("", secs)).asInstanceOf[Date] mustEqual new Date(secs*1000L)
      exp.eval(Array("", "")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 1000)
      exp.eval(Array("", "abcd")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
    }
    "handle exceptions to second conversions with null defaults" >> {
      val exp = Expression("try(secsToDate($1), null)")
      val secs = 100L
      exp.eval(Array("", secs)).asInstanceOf[Date] mustEqual new Date(secs*1000L)
      exp.eval(Array("", "")).asInstanceOf[Date] must beNull
      exp.eval(Array("", "abcd")).asInstanceOf[Date] must beNull
    }
    "allow spaces in try statements" >> {
      foreach(Seq("try($1::int,0)", "try ( $1::int, 0 )")) { t =>
        val exp = Expression(t)
        exp.eval(Array("", "1")).asInstanceOf[Int] mustEqual 1
        exp.eval(Array("", "")).asInstanceOf[Int] mustEqual 0
        exp.eval(Array("", "abcd")).asInstanceOf[Int] mustEqual 0
      }
    }
    "add" >> {
      val exp1 = Expression("add($1,$2)")
      exp1.eval(Array("","1","2")) mustEqual 3.0
      exp1.eval(Array("","-1","2")) mustEqual 1.0

      val exp2 = Expression("add($1,$2,$3)")
      exp2.eval(Array("","1","2","3.0")) mustEqual 6.0
      exp2.eval(Array("","-1","2","3.0")) mustEqual 4.0
    }
    "multiply" >> {
      val exp1 = Expression("multiply($1,$2)")
      exp1.eval(Array("","1","2")) mustEqual 2.0
      exp1.eval(Array("","-1","2")) mustEqual -2.0

      val exp2 = Expression("multiply($1,$2,$3)")
      exp2.eval(Array("","1","2","3.0")) mustEqual 6.0
      exp2.eval(Array("","-1","2","3.0")) mustEqual -6.0
    }
    "subtract" >> {
      val exp1 = Expression("subtract($1,$2)")
      exp1.eval(Array("","2","1")) mustEqual 1.0
      exp1.eval(Array("","-1","2")) mustEqual -3.0

      val exp2 = Expression("subtract($1,$2,$3)")
      exp2.eval(Array("","1","2","3.0")) mustEqual -4.0
      exp2.eval(Array("","-1","2","3.0")) mustEqual -6.0
    }
    "divide" >> {
      val exp1 = Expression("divide($1,$2)")
      exp1.eval(Array("","2","1")) mustEqual 2.0
      exp1.eval(Array("","-1","2")) mustEqual -0.5

      val exp2 = Expression("divide($1,$2,$3)")
      exp2.eval(Array("","1","2","3.0")) mustEqual (1.0/2/3) // 0.166666666666
      exp2.eval(Array("","-1","2","3.0")) mustEqual (-1.0/2/3) // -0.166666666666
    }
    "find mean" >> {
      val exp1 = Expression("mean($1,$2,$3,$4)")
      exp1.eval(Array("","1","2","3","4")) mustEqual 2.5
    }
    "find min" >> {
      val exp1 = Expression("min($1,$2,$3,$4)::int")
      exp1.eval(Array("","1","2","3","4")) mustEqual 1
    }
    "find max" >> {
      val exp1 = Expression("max($1,$2,$3,$4)::int")
      exp1.eval(Array("","1","2","3","4")) mustEqual 4
    }
    "allow for number formatting using printf" >> {
      val exp = Expression("printf('%.2f', divide($1,$2,$3))")
      exp.eval(Array("","-1","2","3.0")) mustEqual "-0.17"
    }
    "allow for number formatting using printf" >> {
      val exp = Expression("printf('%.2f', divide(-1, 2, 3))")
      exp.eval(Array()) mustEqual "-0.17"
    }
    "support cql buffer" >> {
      val exp = Expression("cql:buffer($1, $2)")
      val buf = exp.eval(Array(null, "POINT(1 1)", 2.0))
      buf must not(beNull)
      buf.getClass mustEqual classOf[Polygon]
      buf.asInstanceOf[Polygon].getCentroid.getX must beCloseTo(1, 0.0001)
      buf.asInstanceOf[Polygon].getCentroid.getY must beCloseTo(1, 0.0001)
      // note: area is not particularly close as there aren't very many points in the polygon
      buf.asInstanceOf[Polygon].getArea must beCloseTo(math.Pi * 4.0, 0.2)
    }
    "convert stringToDouble zero default" >> {
      val exp = Expression("stringToDouble($1, 0.0)")
      exp.eval(Array("", "1.2")) mustEqual 1.2
      exp.eval(Array("", "")) mustEqual 0.0
      exp.eval(Array("", null)) mustEqual 0.0
      exp.eval(Array("", "notadouble")) mustEqual 0.0
    }
    "convert stringToDouble null default" >> {
      val exp = Expression("stringToDouble($1, $2)")
      exp.eval(Array("", "1.2", null)) mustEqual 1.2
      exp.eval(Array("", "", null)) mustEqual null
      exp.eval(Array("", null, null)) mustEqual null
      exp.eval(Array("", "notadouble", null)) mustEqual null
    }
    "convert stringToInt zero default" >> {
      foreach(Seq("stringToInt", "stringToInteger")) { fn =>
        val exp = Expression(s"$fn($$1, 0)")
        exp.eval(Array("", "2")) mustEqual 2
        exp.eval(Array("", "")) mustEqual 0
        exp.eval(Array("", null)) mustEqual 0
        exp.eval(Array("", "1.2")) mustEqual 0
      }
    }
    "convert stringToInt null default" >> {
      foreach(Seq("stringToInt", "stringToInteger")) { fn =>
        val exp = Expression(s"$fn($$1, $$2)")
        exp.eval(Array("", "2", null)) mustEqual 2
        exp.eval(Array("", "", null)) mustEqual null
        exp.eval(Array("", null, null)) mustEqual null
        exp.eval(Array("", "1.2", null)) mustEqual null
      }
    }
    "convert stringToLong zero default" >> {
      val exp = Expression("stringToLong($1, 0L)")
      exp.eval(Array("", "22960000000")) mustEqual 22960000000L
      exp.eval(Array("", "")) mustEqual 0L
      exp.eval(Array("", null)) mustEqual 0L
      exp.eval(Array("", "1.2")) mustEqual 0L
    }
    "convert stringToLong null default" >> {
      val exp = Expression("stringToLong($1, $2)")
      exp.eval(Array("", "22960000000", null)) mustEqual 22960000000L
      exp.eval(Array("", "", null)) mustEqual null
      exp.eval(Array("", null, null)) mustEqual null
      exp.eval(Array("", "1.2", null)) mustEqual null
    }
    "convert stringToFloat zero default" >> {
      val exp = Expression("stringToFloat($1, 0.0f)")
      exp.eval(Array("", "1.2")) mustEqual 1.2f
      exp.eval(Array("", "")) mustEqual 0.0f
      exp.eval(Array("", null)) mustEqual 0.0f
      exp.eval(Array("", "notafloat")) mustEqual 0.0f
    }
    "convert stringToFloat zero default" >> {
      val exp = Expression("stringToFloat($1, $2)")
      exp.eval(Array("", "1.2", null)) mustEqual 1.2f
      exp.eval(Array("", "", null)) mustEqual null
      exp.eval(Array("", null, null)) mustEqual null
      exp.eval(Array("", "notafloat", null)) mustEqual null
    }
    "convert stringToBoolean false default" >> {
      val exp = Expression("stringToBoolean($1, false)")
      exp.eval(Array("", "true")) mustEqual true
      exp.eval(Array("", "")) mustEqual false
      exp.eval(Array("", null)) mustEqual false
      exp.eval(Array("", "18")) mustEqual false
    }
    "convert stringToBoolean null default" >> {
      val exp = Expression("stringToBoolean($1,$2)")
      exp.eval(Array("", "true", null)) mustEqual true
      exp.eval(Array("", "", null)) mustEqual null
      exp.eval(Array("", null, null)) mustEqual null
      exp.eval(Array("", "18", null)) mustEqual null
    }
    "return null for non-existing fields" >> {
      val fieldsCtx = new EvaluationContextImpl(IndexedSeq("foo", "bar"), Array("5", "10"), null, Map.empty)
      Expression("$b").eval(Array())(fieldsCtx) mustEqual null
      Expression("$bar").eval(Array())(fieldsCtx) mustEqual "10"
    }
    "create lists" >> {
      val trans = Expression("list($0, $1, $2)")
      val res = trans.eval(Array("a", "b", "c")).asInstanceOf[java.util.List[String]]
      res.size() mustEqual 3
      res.asScala mustEqual List("a", "b", "c")
    }
    "parse lists with default delimiter" >> {
      val trans = Expression("parseList('string', $0)")
      val res = trans.eval(Array("a,b,c")).asInstanceOf[java.util.List[String]]
      res.size mustEqual 3
      res.asScala mustEqual List("a", "b", "c")
    }
    "parse lists with custom delimiter" >> {
      val trans = Expression("parseList('string', $0, '%')")
      val res = trans.eval(Array("a%b%c")).asInstanceOf[java.util.List[String]]
      res.size mustEqual 3
      res.asScala mustEqual List("a", "b", "c")
    }
    "parse lists with numbers" >> {
      val trans = Expression("parseList('int', $0, '%')")
      val res = trans.eval(Array("1%2%3")).asInstanceOf[java.util.List[Int]]
      res.size mustEqual 3
      res.asScala mustEqual List(1,2,3)
    }
    "throw exception for invalid list values" >> {
      val trans = Expression("parseList('int', $0, '%')")
      trans.eval(Array("1%2%a")).asInstanceOf[java.util.List[Int]] must throwAn[IllegalArgumentException]
    }
    "parse maps with default delimiter" >> {
      val trans = Expression("parseMap('String->Int', $0)")
      val res = trans.eval(Array("a->1,b->2,c->3"))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.Map[String, Int]]
      res.asInstanceOf[java.util.Map[String, Int]].size mustEqual 3
      res.asInstanceOf[java.util.Map[String, Int]].asScala mustEqual Map("a" -> 1, "b" -> 2, "c" -> 3)
    }
    "parse maps with custom delimiter" >> {
      val trans = Expression("parseMap('String->Int', $0, '%', ';')")
      val res = trans.eval(Array("a%1;b%2;c%3"))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.Map[String, Int]]
      res.asInstanceOf[java.util.Map[String, Int]].size mustEqual 3
      res.asInstanceOf[java.util.Map[String, Int]].asScala mustEqual Map("a" -> 1, "b" -> 2, "c" -> 3)
    }
    "throw exception for invalid map values" >> {
      val trans = Expression("parseMap('String->Int', $0)")
      trans.eval(Array("a->1,b->2,c->d")) must throwAn[IllegalArgumentException]
    }
    "handle default values" >> {
      val trans = Expression("withDefault($0, 'foo')")
      trans.eval(Array(null)) mustEqual "foo"
      trans.eval(Array("bar")) mustEqual "bar"
      trans.eval(Array("")) mustEqual ""
    }
    "strip quotes" >> {
      val trans = Expression("stripQuotes($0)")
      trans.eval(Array("'foo'")) mustEqual "foo"
      trans.eval(Array("\"foo'")) mustEqual "foo"
      trans.eval(Array("\"'foo'")) mustEqual "foo"

      // white space is preserved
      trans.eval(Array("'foo\t\t")) mustEqual "foo\t\t"
      trans.eval(Array("  foo'\"\"")) mustEqual "  foo"
    }
    "strip whitespace" >> {
      val trans = Expression("strip($0)")
      trans.eval(Array("\t   foo   \t\t\t")) mustEqual "foo"
    }
    "strip from start and end with strip" >> {
      val trans = Expression("strip($0, '\\'')")
      trans.eval(Array("'foo'")) mustEqual "foo"
      trans.eval(Array("'foo")) mustEqual "foo"
      trans.eval(Array("foo'")) mustEqual "foo"
    }
    "strip multiple chars (e.g. quotes)" >> {
      val trans = Expression("strip($0, '\\'\\\"')")
      trans.eval(Array("'foo'")) mustEqual "foo"
      trans.eval(Array("\"foo'")) mustEqual "foo"
      trans.eval(Array("\"'foo'")) mustEqual "foo"
      trans.eval(Array("'foo")) mustEqual "foo"
      trans.eval(Array("foo'\"\"")) mustEqual "foo"
    }
    "strip prefix only" >> {
      val trans = Expression("stripPrefix($0, '\\'')")
      trans.eval(Array("'foo'", "'")) mustEqual "foo'"
      trans.eval(Array("'foo", "'")) mustEqual "foo"
      trans.eval(Array("foo'", "'")) mustEqual "foo'"
      trans.eval(Array(" 'foo'", "'")) mustEqual " 'foo'"
    }
    "strip prefix with  multiple chars" >> {
      val trans = Expression("stripPrefix($0, '\\'\\\"')")
      trans.eval(Array("'foo'", "'")) mustEqual "foo'"
      trans.eval(Array("'foo", "'")) mustEqual "foo"
      trans.eval(Array("foo'", "'")) mustEqual "foo'"
      trans.eval(Array("\"'foo\"", "'")) mustEqual "foo\""
      trans.eval(Array("\"\"\"'foo'", "'")) mustEqual "foo'"
    }
    "strip suffix only" >> {
      val trans = Expression("stripSuffix($0, '\\'')")
      trans.eval(Array("'foo'")) mustEqual "'foo"
      trans.eval(Array("'foo")) mustEqual "'foo"
      trans.eval(Array("foo'")) mustEqual "foo"
    }
    "strip suffix with preserving whitespace" >> {
      val trans = Expression("stripSuffix($0, 'ab')")
      trans.eval(Array("fooab ")) mustEqual "fooab "
    }
    "strip suffix multiple chars" >> {
      val trans = Expression("stripSuffix($0, '\\'\\\"')")
      trans.eval(Array("'\"foo'")) mustEqual "'\"foo"
      trans.eval(Array("'\"foo")) mustEqual "'\"foo"
      trans.eval(Array("\"foo'")) mustEqual "\"foo"
      trans.eval(Array("'foo\"'")) mustEqual "'foo"
      trans.eval(Array("'foo\"")) mustEqual "'foo"
      trans.eval(Array("foo'\"")) mustEqual "foo"
    }
    "strip something other than quotes" >> {
      val trans = Expression("strip($0, 'X')")
      trans.eval(Array("XfooX")) mustEqual "foo"
      trans.eval(Array("Xfoo")) mustEqual "foo"
      trans.eval(Array("fooX")) mustEqual "foo"
    }
    "remove strings" >> {
      val trans = Expression("remove($0, '\\'')")
      trans.eval(Array("'foo'")) mustEqual "foo"
      trans.eval(Array("'foo")) mustEqual "foo"
      trans.eval(Array("foo'")) mustEqual "foo"
      trans.eval(Array("f'o'o'")) mustEqual "foo"
      Expression("remove($0, 'abc')").eval(Array("foabco")) mustEqual "foo"
    }
    "replace" >> {
      val trans = Expression("replace($0, '\\'', '\\\"')")
      trans.eval(Array("'foo'")) mustEqual "\"foo\""
      trans.eval(Array("'foo")) mustEqual "\"foo"
      trans.eval(Array("foo'")) mustEqual "foo\""
      trans.eval(Array("f'o'o'")) mustEqual "f\"o\"o\""
      Expression("replace($0, 'a', 'o')").eval(Array("faa")) mustEqual "foo"
    }
  }
}
