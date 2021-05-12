/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

<<<<<<< HEAD
=======
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.{Collections, Date}

>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
import org.apache.commons.codec.binary.Base64
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.AbstractConverter.BasicField
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.convert2.transforms.Expression.{FunctionExpression, Literal}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.{Collections, Date}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class ExpressionTest extends Specification {

  import scala.collection.JavaConverters._

  implicit val ctx: EvaluationContext = EvaluationContext.empty

  val testDate = Converters.convert("2015-01-01T00:00:00.000Z", classOf[Date])

  val testBytes = {
    val bytes = Array.ofDim[Byte](32)
    new Random(-9L).nextBytes(bytes)
    bytes
  }

  "Transformers" should {

    "allow literal strings" >> {
      val exp = Expression("'hello'")
      exp.apply(null) must be equalTo "hello"
    }
    "allow quoted strings" >> {
      val exp = Expression("'he\\'llo'")
      exp.apply(null) must be equalTo "he'llo"
    }
    "allow empty literal strings" >> {
      val exp = Expression("''")
      exp.apply(null) must be equalTo ""
    }
    "allow native ints" >> {
      val exp = Expression("1")
      val res = exp.apply(null)
      res must not(beNull)
      res.getClass mustEqual classOf[java.lang.Integer]
      res mustEqual 1
    }
    "allow native longs" >> {
      val exp = Expression("1L")
      val res = exp.apply(null)
      res must not(beNull)
      res.getClass mustEqual classOf[java.lang.Long]
      res mustEqual 1L
    }
    "allow native floats" >> {
      val tests = Seq(("1.0", 1f), ("1.0", 1f), (".1", .1f), ("0.1", .1f), ("-1.0", -1f))
      foreach(tests) { case (s, expected) =>
        foreach(Seq("f", "F")) { suffix =>
          val exp = Expression(s + suffix)
          val res = exp.apply(null)
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
          val exp = Expression(s + suffix)
          val res = exp.apply(null)
          res must not(beNull)
          res.getClass mustEqual classOf[java.lang.Double]
          res mustEqual expected
        }
      }
    }
    "allow native booleans" >> {
      Expression("false").apply(null) mustEqual false
      Expression("true").apply(null) mustEqual true
    }
    "allow native nulls" >> {
      Expression("null").apply(null) must beNull
    }
    "trim" >> {
      val exp = Expression("trim($1)")
<<<<<<< HEAD
      exp.apply(Array("", "foo ", "bar")) must be equalTo "foo"
      exp.apply(Array("", null)) must beNull
    }
    "capitalize" >> {
      val exp = Expression("capitalize($1)")
      exp.apply(Array("", "foo", "bar")) must be equalTo "Foo"
      exp.apply(Array("", null)) must beNull
    }
    "lowercase" >> {
      val exp = Expression("lowercase($1)")
      exp.apply(Array("", "FOO", "bar")) must be equalTo "foo"
      exp.apply(Array("", null)) must beNull
    }
    "uppercase" >> {
      val exp = Expression("uppercase($1)")
      exp.apply(Array("", "FoO")) must be equalTo "FOO"
      exp.apply(Array("", null)) must beNull
    }
    "regexReplace" >> {
      val exp = Expression("regexReplace('foo'::r,'bar',$1)")
      exp.apply(Array("", "foobar")) must be equalTo "barbar"
      exp.apply(Array("", null)) must beNull
    }
    "compound expressions" >> {
      val exp = Expression("regexReplace('foo'::r,'bar',trim($1))")
      exp.apply(Array("", " foobar ")) must be equalTo "barbar"
      exp.apply(Array("", null)) must beNull
=======
      exp.eval(Array("", "foo ", "bar")) must be equalTo "foo"
      exp.eval(Array("", null)) must beNull
    }
    "capitalize" >> {
      val exp = Expression("capitalize($1)")
      exp.eval(Array("", "foo", "bar")) must be equalTo "Foo"
      exp.eval(Array("", null)) must beNull
    }
    "lowercase" >> {
      val exp = Expression("lowercase($1)")
      exp.eval(Array("", "FOO", "bar")) must be equalTo "foo"
      exp.eval(Array("", null)) must beNull
    }
    "uppercase" >> {
      val exp = Expression("uppercase($1)")
      exp.eval(Array("", "FoO")) must be equalTo "FOO"
      exp.eval(Array("", null)) must beNull
    }
    "regexReplace" >> {
      val exp = Expression("regexReplace('foo'::r,'bar',$1)")
      exp.eval(Array("", "foobar")) must be equalTo "barbar"
      exp.eval(Array("", null)) must beNull
    }
    "compound expressions" >> {
      val exp = Expression("regexReplace('foo'::r,'bar',trim($1))")
      exp.eval(Array("", " foobar ")) must be equalTo "barbar"
      exp.eval(Array("", null)) must beNull
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    }
    "take substrings" >> {
      foreach(Seq("substring", "substr")) { fn =>
        val exp = Expression(s"$fn($$1, 2, 5)")
<<<<<<< HEAD
        exp.apply(Array("", "foobarbaz")) must be equalTo "foobarbaz".substring(2, 5)
        exp.apply(Array("", null)) must beNull
=======
        exp.eval(Array("", "foobarbaz")) must be equalTo "foobarbaz".substring(2, 5)
        exp.eval(Array("", null)) must beNull
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
      }
    }
    "calculate strlen" >> {
      val exp = Expression("strlen($1)")
<<<<<<< HEAD
      exp.apply(Array("", "FOO")) mustEqual 3
      exp.apply(Array("", null)) mustEqual 0
    }
    "calculate length" >> {
      val exp = Expression("length($1)")
      exp.apply(Array("", "FOO")) mustEqual 3
      exp.apply(Array("", null)) mustEqual 0
    }
    "convert toString" >> {
      val exp = Expression("toString($1)")
      exp.apply(Array("", Int.box(5))) must be equalTo "5"
      exp.apply(Array("", null)) must beNull
=======
      exp.eval(Array("", "FOO")) must be equalTo 3
      exp.eval(Array("", null)) mustEqual 0
    }
    "calculate length" >> {
      val exp = Expression("length($1)")
      exp.eval(Array("", "FOO")) must be equalTo 3
      exp.eval(Array("", null)) mustEqual 0
    }
    "convert toString" >> {
      val exp = Expression("toString($1)")
      exp.eval(Array("", 5)) must be equalTo "5"
      exp.eval(Array("", null)) must beNull
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    }
    "concat with toString" >> {
      val exp = Expression("concat(toString($1), toString($2))")
      exp.apply(Array("", Int.box(5), Int.box(6))) must be equalTo "56"
    }
    "concat many args" >> {
      val exp = Expression("concat($1, $2, $3, $4, $5, $6)")
<<<<<<< HEAD
      exp.apply(Array("", Int.box(1), Int.box(2), Int.box(3), Int.box(4), Int.box(5), Int.box(6))) mustEqual "123456"
      exp.apply(Array("", Int.box(1), null, Int.box(3), Int.box(4), Int.box(5), Int.box(6))) mustEqual "1null3456"
    }
    "mkstring" >> {
      val exp = Expression("mkstring(',', $1, $2, $3, $4, $5, $6)")
      exp.apply(Array("", Int.box(1), Int.box(2), Int.box(3), Int.box(4), Int.box(5), Int.box(6))) must be equalTo "1,2,3,4,5,6"
      exp.apply(Array("", Int.box(1), null, Int.box(3), Int.box(4), Int.box(5), Int.box(6))) must be equalTo "1,null,3,4,5,6"
=======
      exp.eval(Array("", 1, 2, 3, 4, 5, 6)) must be equalTo "123456"
      exp.eval(Array("", 1, null, 3, 4, 5, 6)) mustEqual "1null3456"
    }
    "mkstring" >> {
      val exp = Expression("mkstring(',', $1, $2, $3, $4, $5, $6)")
      exp.eval(Array("", 1, 2, 3, 4, 5, 6)) must be equalTo "1,2,3,4,5,6"
      exp.eval(Array("", 1, null, 3, 4, 5, 6)) must be equalTo "1,null,3,4,5,6"
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    }
    "convert emptyToNull" >> {
      val exp = Expression("emptyToNull($1)")
      exp.apply(Array("", "foo")) mustEqual "foo"
      exp.apply(Array("", "")) must beNull
      exp.apply(Array("", "  ")) must beNull
      exp.apply(Array("", null)) must beNull
    }
    "printf" >> {
      val exp = Expression("printf('%s-%s-%sT00:00:00.000Z', '2015', '01', '01')")
      exp.apply(Array()) mustEqual "2015-01-01T00:00:00.000Z"
    }
    "handle non string ints" >> {
      val exp = Expression("$2")
      exp.apply(Array("", "1", Int.box(2))) mustEqual 2
    }
    "cast to int" >> {
      foreach(Seq("int", "integer")) { cast =>
        val exp = Expression(s"$$1::$cast")
        exp.apply(Array("", "1")) mustEqual 1
        exp.apply(Array("", Double.box(1E2))) mustEqual 100
        exp.apply(Array("", Int.box(1))) mustEqual 1
        exp.apply(Array("", Double.box(1D))) mustEqual 1
        exp.apply(Array("", Float.box(1F))) mustEqual 1
        exp.apply(Array("", Long.box(1L))) mustEqual 1
        exp.apply(Array("", null)) must throwA[NullPointerException]
      }
    }
    "cast to long" >> {
      val exp = Expression("$1::long")
      exp.apply(Array("", "1")) mustEqual 1L
      exp.apply(Array("", Double.box(1E2))) mustEqual 100L
      exp.apply(Array("", Int.box(1))) mustEqual 1L
      exp.apply(Array("", Double.box(1D))) mustEqual 1L
      exp.apply(Array("", Float.box(1F))) mustEqual 1L
      exp.apply(Array("", Long.box(1L))) mustEqual 1L
      exp.apply(Array("", null)) must throwA[NullPointerException]
    }
    "cast to float" >> {
      val exp = Expression("$1::float")
      exp.apply(Array("", "1")) mustEqual 1F
      exp.apply(Array("", Double.box(1E2))) mustEqual 100F
      exp.apply(Array("", Int.box(1))) mustEqual 1F
      exp.apply(Array("", Double.box(1D))) mustEqual 1F
      exp.apply(Array("", Float.box(1F))) mustEqual 1F
      exp.apply(Array("", Long.box(1L))) mustEqual 1F
      exp.apply(Array("", null)) must throwA[NullPointerException]
    }
    "cast to double" >> {
      val exp = Expression("$1::double")
      exp.apply(Array("", "1")) mustEqual 1D
      exp.apply(Array("", Double.box(1E2))) mustEqual 100D
      exp.apply(Array("", Int.box(1))) mustEqual 1D
      exp.apply(Array("", Double.box(1D))) mustEqual 1D
      exp.apply(Array("", Float.box(1F))) mustEqual 1D
      exp.apply(Array("", Long.box(1L))) mustEqual 1D
      exp.apply(Array("", null)) must throwA[NullPointerException]
    }
    "cast to boolean" >> {
      foreach(Seq("bool", "boolean")) { cast =>
        val exp = Expression(s"$$1::$cast")
        exp.apply(Array("", "true")) mustEqual true
        exp.apply(Array("", "false")) mustEqual false
        exp.apply(Array("", null)) must throwA[NullPointerException]
      }
    }
    "cast to string" >> {
      val exp = Expression("$1::string")
      exp.apply(Array("", "1")) mustEqual "1"
      exp.apply(Array("", Int.box(1))) mustEqual "1"
      exp.apply(Array("", null)) must throwA[NullPointerException]
    }
    "parse dates with custom format" >> {
      val exp = Expression("date('yyyyMMdd', $1)")
      exp.apply(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse dates with a realistic custom format" >> {
      val exp = Expression("date('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', $1)")
      exp.apply(Array("", "2015-01-01T00:00:00.000000")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse datetime" >> {
      foreach(Seq("datetime", "dateTime")) { cast =>
        val exp = Expression(s"$cast($$1)")
        exp.apply(Array("", "2015-01-01T00:00:00.000Z")).asInstanceOf[Date] must be equalTo testDate
      }
    }
    "parse isoDate" >> {
      val exp = Expression("isoDate($1)")
      exp.apply(Array("", "2015-01-01")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse basicDate" >> {
      val exp = Expression("basicDate($1)")
      exp.apply(Array("", "20150101")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse isoDateTime" >> {
      val exp = Expression("isoDateTime($1)")
      exp.apply(Array("", "2015-01-01T00:00:00")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse basicDateTime" >> {
      val exp = Expression("basicDateTime($1)")
      exp.apply(Array("", "20150101T000000.000Z")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse basicDateTimeNoMillis" >> {
      val exp = Expression("basicDateTimeNoMillis($1)")
      exp.apply(Array("", "20150101T000000Z")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse dateHourMinuteSecondMillis" >> {
      val exp = Expression("dateHourMinuteSecondMillis($1)")
      exp.apply(Array("", "2015-01-01T00:00:00.000")).asInstanceOf[Date] must be equalTo testDate
    }
    "parse millisToDate" >> {
      val millis = Long.box(testDate.getTime)
      val exp = Expression("millisToDate($1)")
      exp.apply(Array("", millis)).asInstanceOf[Date] must be equalTo testDate
    }
    "parse secsToDate" >> {
      val secs = Long.box(testDate.getTime / 1000L)
      val exp = Expression("secsToDate($1)")
      exp.apply(Array("", secs)).asInstanceOf[Date] must be equalTo testDate
    }
    "parse null dates" >> {
      val input = Array("", null)
      val expressions = Seq(
        "date('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', $1)",
        "isoDate($1)",
        "basicDate($1)",
        "isoDateTime($1)",
        "basicDateTime($1)",
        "dateTime($1)",
        "basicDateTimeNoMillis($1)",
        "dateHourMinuteSecondMillis($1)",
        "millisToDate($1)",
        "secsToDate($1)"
      )
      foreach(expressions) { expression =>
        Expression(expression).apply(input) must beNull
        Expression(s"require($expression)").apply(input) must throwAn[IllegalArgumentException]
      }
    }
    "transform a date to a string" >> {
      val d = LocalDateTime.now()
      val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
      val exp = Expression("dateToString('yyyy-MM-dd\\'T\\'HH:mm:ss.SSS', $1)")
      exp.apply(Array("", Date.from(d.toInstant(ZoneOffset.UTC)))).asInstanceOf[String] must be equalTo fmt.format(d)
    }
    "transform a date to milliseconds" >> {
      val d = new Date(9999000)
      val exp = Expression("dateToMillis($1)")
      exp.apply(Array("", d)) mustEqual d.getTime
    }
    "parse date strings from printf" >> {
      val exp = Expression("datetime(printf('%s-%s-%sT00:00:00.000Z', $1, $2, $3))")
      exp.apply(Array("", "2015", "01", "01")) must be equalTo testDate
    }
    "parse point geometries" >> {
      val exp = Expression("point($1, $2)")
      exp.apply(Array("", Double.box(45.0), Double.box(46.0))) mustEqual WKTUtils.read("POINT(45 46)")

      val trans = Expression("point($0)")
      trans.apply(Array("POINT(50 52)")) mustEqual WKTUtils.read("POINT(50 52)")

      val z = Expression("point($1,$2,$3)")
      z.apply(Array("", Double.box(45.0), Double.box(46.0), Double.box(47))).asInstanceOf[Point]
          .getCoordinate.toString mustEqual new Coordinate(45.0, 46.0, 47).toString

      val zm = Expression("point($1,$2,$3,$4)")
      zm.apply(Array("", Double.box(45.0), Double.box(46.0), Double.box(47), Double.box(48))).asInstanceOf[Point]
          .getCoordinate.toString mustEqual new CoordinateXYZM(45.0, 46.0, 47, 48).toString

      val m = Expression("pointM($1,$2,$3)")
      m.apply(Array("", Double.box(45.0), Double.box(46.0), Double.box(47))).asInstanceOf[Point]
          .getCoordinate.toString mustEqual new CoordinateXYM(45.0, 46.0, 47).toString
    }
    "parse multipoint wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val multiPoint = geoFac.createMultiPointFromCoords(Array(new Coordinate(45.0, 45.0), new Coordinate(50, 52)))
      val trans = Expression("multipoint($0)")
      trans.apply(Array("Multipoint((45.0 45.0), (50 52))")).asInstanceOf[MultiPoint] mustEqual multiPoint

      // convert objects
      val geom = multiPoint.asInstanceOf[Geometry]
      val res = trans.apply(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[MultiPoint]
      res.asInstanceOf[MultiPoint] mustEqual WKTUtils.read("Multipoint((45.0 45.0), (50 52))")
    }
    "parse multipoint from x/y coords" >> {
      val expected = WKTUtils.read("MultiPoint((45.0 45.0), (50 52))")
      expected must not(beNull)
      val trans = Expression("multipoint($0, $1)")
      val x = Seq(45, 50).asJava
      val y = Seq(45, 52).asJava
      trans.apply(Array(x, y)) mustEqual expected
    }
    "parse linestring wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val lineStr = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      val trans = Expression("linestring($0)")
      trans.apply(Array("Linestring(102 0, 103 1, 104 0, 105 1)")).asInstanceOf[LineString] mustEqual lineStr

      // type conversion
      val geom = lineStr.asInstanceOf[Geometry]
      val res = trans.apply(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[LineString]
      res.asInstanceOf[LineString] mustEqual WKTUtils.read("Linestring(102 0, 103 1, 104 0, 105 1)")
    }
    "parse linestring from x/y coords" >> {
      val expected = WKTUtils.read("Linestring(102 0, 103 1, 104 0, 105 1)")
      expected must not(beNull)
      val trans = Expression("linestring($0, $1)")
      val x = Seq(102, 103, 104, 105).asJava
      val y = Seq(0, 1, 0, 1).asJava
      trans.apply(Array(x, y)) mustEqual expected
    }
    "parse multilinestring wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val multiLineStr = geoFac.createMultiLineString(Array(
        geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray),
          geoFac.createLineString(Seq((0, 0), (1, 2), (2, 3), (4, 5)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      ))
      val trans = Expression("multilinestring($0)")
      trans.apply(Array("MultiLinestring((102 0, 103 1, 104 0, 105 1), (0 0, 1 2, 2 3, 4 5))")).asInstanceOf[MultiLineString] mustEqual multiLineStr

      // type conversion
      val geom = multiLineStr.asInstanceOf[Geometry]
      val res = trans.apply(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[MultiLineString]
      res.asInstanceOf[MultiLineString] mustEqual WKTUtils.read("MultiLinestring((102 0, 103 1, 104 0, 105 1), (0 0, 1 2, 2 3, 4 5))")
    }
    "parse polygon wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val poly = geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      val trans = Expression("polygon($0)")
      trans.apply(Array("polygon((100 0, 101 0, 101 1, 100 1, 100 0))")).asInstanceOf[Polygon] mustEqual poly

      // type conversion
      val geom = poly.asInstanceOf[Polygon]
      val res = trans.apply(Array(geom))
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
      trans.apply(Array("multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), ((10 0, 11 0, 11 1, 10 1, 10 0)))")).asInstanceOf[MultiPolygon] mustEqual multiPoly

      // type conversion
      val geom = multiPoly.asInstanceOf[MultiPolygon]
      val res = trans.apply(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[MultiPolygon]
      res.asInstanceOf[MultiPolygon] mustEqual WKTUtils.read("multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), ((10 0, 11 0, 11 1, 10 1, 10 0)))")
    }
    "parse geometry wkt and objects" >> {
      val geoFac = new GeometryFactory()
      val lineStr = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
      val trans = Expression("geometry($0)")
      trans.apply(Array("Linestring(102 0, 103 1, 104 0, 105 1)")).asInstanceOf[Geometry] must be equalTo lineStr

      // type conversion
      val geom = lineStr.asInstanceOf[Geometry]
      val res = trans.apply(Array(geom))
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
      trans.apply(Array("GeometryCollection(Linestring(102 0, 103 1, 104 0, 105 1)," +
          "multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), " +
          "((10 0, 11 0, 11 1, 10 1, 10 0))))")).asInstanceOf[GeometryCollection] mustEqual geoCol

      // type conversion
      val geom = geoCol.asInstanceOf[Geometry]
      val res = trans.apply(Array(geom))
      res must not(beNull)
      res.getClass mustEqual classOf[GeometryCollection]
      res.asInstanceOf[GeometryCollection] mustEqual WKTUtils.read(
        "GeometryCollection(Linestring(102 0, 103 1, 104 0, 105 1), " +
        "multipolygon(((100 0, 101 0, 101 1, 100 1, 100 0)), ((10 0, 11 0, 11 1, 10 1, 10 0))))")
    }
    "parse null geometries" >> {
      val functions =
        Seq(
          "point($0)",
          "point($0, $1)",
          "linestring($0)",
          "multipoint($0)",
          "polygon($0)",
          "multilinestring($0)",
          "multipolygon($0)",
          "geometrycollection($0)",
          "geometry($0)"
        )
<<<<<<< HEAD
      foreach(functions.map(Expression.apply)) { exp =>
        exp.apply(Array(null, null)) must beNull
=======
      foreach(functions) { exp =>
        Expression(exp).eval(Array(null, null)) must beNull
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
      }
    }
    "reproject to EPSG 4326" >> {
      val geom = WKTUtils.read("POINT (1113194.91 1689200.14)")
      val trans = Expression("projectFrom('EPSG:3857',$1)")
      val transformed = trans.apply(Array("", geom))
      transformed must not(beNull)
      transformed.getClass mustEqual classOf[Point]
      transformed.asInstanceOf[Point].getX must beCloseTo(10d, 0.001)
      transformed.asInstanceOf[Point].getY must beCloseTo(15d, 0.001)
    }
    "generate md5 hashes" >> {
      val exp = Expression("md5($0)")
<<<<<<< HEAD
      exp.apply(Array(testBytes)) mustEqual "53587708703184a0b6f8952425c21d9f"
    }
    "generate murmur hashes" >> {
      val exp = Expression("murmurHash3($0)")
      exp.apply(Array("foo")) mustEqual "6145f501578671e2877dba2be487af7e"
      exp.apply(Array("foo".getBytes)) mustEqual "6145f501578671e2877dba2be487af7e"
=======
      val hashedResult = exp.eval(Array(testBytes)).asInstanceOf[String]
      hashedResult mustEqual "53587708703184a0b6f8952425c21d9f"
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    }
    "generate uuids" >> {
      val exp = Expression("uuid()")
      val res = exp.apply(null)
      res must not(beNull)
      res.getClass mustEqual classOf[String]
    }
    "generate z3 uuids" >> {
      val exp = Expression("uuidZ3($0, $1, 'week')")
      val geom = WKTUtils.read("POINT (103 1)")
      val date = Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])
      val res = exp.apply(Array(geom, date))
      res must not(beNull)
      res.getClass mustEqual classOf[String]
    }
    "generate z3 centroid uuids" >> {
      val exp = Expression("uuidZ3Centroid($0, $1, 'week')")
      val geom = WKTUtils.read("LINESTRING (102 0, 103 1, 104 0, 105 1)")
      val date = Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])
      val res = exp.apply(Array(geom, date))
      res must not(beNull)
      res.getClass mustEqual classOf[String]
    }
    "encode bytes as base64 strings" >> {
      foreach(Seq("base64($0)", "base64Encode($0)")) { expression =>
        val exp = Expression(expression)
        exp.apply(Array(testBytes)) mustEqual Base64.encodeBase64URLSafeString(testBytes)
      }
    }
    "decode base64 strings as bytes" >> {
      val encoded = Base64.encodeBase64URLSafeString(testBytes)
      val exp = Expression("base64Decode($0)")
      exp.apply(Array(encoded)) mustEqual testBytes
    }
    "handle whitespace in functions" >> {
      val variants = Seq(
        "printf('%s-%s-%sT00:00:00.000Z', $1, $2, $3)",
        "printf ( '%s-%s-%sT00:00:00.000Z' , $1 , $2 , $3 )"
      )
      foreach(variants) { t =>
        val exp = Expression(t)
        exp.apply(Array("", "2015", "01", "01")) mustEqual "2015-01-01T00:00:00.000Z"
      }
    }
    "handle named values" >> {
      val fields = Seq(
        BasicField("baz", None),
        BasicField("foo", Some(Expression("$1"))),
        BasicField("bar", Some(Expression("capitalize($foo)")))
      )
      val metrics = ConverterMetrics.empty
      val ctx = EvaluationContext(fields, Map.empty, Map.empty, metrics, metrics.counter("s"), metrics.counter("f"))

      val result = ctx.evaluate(Array("", "bar"))
      result must beRight
      result.right.get mustEqual Array("", "bar", "Bar")
    }
    "handle named values with spaces and dots" >> {
      val fields = Seq(
        BasicField("foo.bar", Some(Expression("$1"))),
        BasicField("foo bar", Some(Expression("$2"))),
        BasicField("dot", Some(Expression("${foo.bar}"))),
        BasicField("space", Some(Expression("${foo bar}")))
      )
      val metrics = ConverterMetrics.empty
      val ctx = EvaluationContext(fields, Map.empty, Map.empty, metrics, metrics.counter("s"), metrics.counter("f"))

      val result = ctx.evaluate(Array("", "baz", "blu"))
      result must beRight
      result.right.get.slice(2, 4) mustEqual Array("baz", "blu")
    }
    "handle exceptions to casting" >> {
      val exp = Expression("try($1::int, 0)")
      exp.apply(Array("", "1")).asInstanceOf[Int] mustEqual 1
      exp.apply(Array("", "")).asInstanceOf[Int] mustEqual 0
      exp.apply(Array("", "abcd")).asInstanceOf[Int] mustEqual 0
    }
    "handle exceptions to millisecond conversions" >> {
      val exp = Expression("try(millisToDate($1), now())")
      val millis = Long.box(100000L)
      exp.apply(Array("", millis)).asInstanceOf[Date] mustEqual new Date(millis)
      exp.apply(Array("", "")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
      exp.apply(Array("", "abcd")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
    }
    "handle exceptions to millisecond conversions with null defaults" >> {
      val exp = Expression("try(millisToDate($1), null)")
      val millis = Long.box(100000L)
      exp.apply(Array("", millis)).asInstanceOf[Date] mustEqual new Date(millis)
      exp.apply(Array("", "")).asInstanceOf[Date] must beNull
      exp.apply(Array("", "abcd")).asInstanceOf[Date] must beNull
    }
    "handle exceptions to second conversions" >> {
      val exp = Expression("try(secsToDate($1), now())")
      val secs = Long.box(100L)
      exp.apply(Array("", secs)).asInstanceOf[Date] mustEqual new Date(secs*1000L)
      exp.apply(Array("", "")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 1000)
      exp.apply(Array("", "abcd")).asInstanceOf[Date].getTime must beCloseTo(System.currentTimeMillis(), 100)
    }
    "handle exceptions to second conversions with null defaults" >> {
      val exp = Expression("try(secsToDate($1), null)")
      val secs = Long.box(100L)
      exp.apply(Array("", secs)).asInstanceOf[Date] mustEqual new Date(secs*1000L)
      exp.apply(Array("", "")).asInstanceOf[Date] must beNull
      exp.apply(Array("", "abcd")).asInstanceOf[Date] must beNull
    }
    "allow spaces in try statements" >> {
      foreach(Seq("try($1::int,0)", "try ( $1::int, 0 )")) { t =>
        val exp = Expression(t)
        exp.apply(Array("", "1")).asInstanceOf[Int] mustEqual 1
        exp.apply(Array("", "")).asInstanceOf[Int] mustEqual 0
        exp.apply(Array("", "abcd")).asInstanceOf[Int] mustEqual 0
      }
    }
    "add" >> {
      val exp1 = Expression("add($1,$2)")
      exp1.apply(Array("","1","2")) mustEqual 3.0
      exp1.apply(Array("","-1","2")) mustEqual 1.0

      val exp2 = Expression("add($1,$2,$3)")
      exp2.apply(Array("","1","2","3.0")) mustEqual 6.0
      exp2.apply(Array("","-1","2","3.0")) mustEqual 4.0
    }
    "add a list" >> {
      val exp = Expression("add($0)")
      exp.apply(Array(Seq("2","3").asJava)) mustEqual 5.0
    }
    "multiply" >> {
      val exp1 = Expression("multiply($1,$2)")
      exp1.apply(Array("","1","2")) mustEqual 2.0
      exp1.apply(Array("","-1","2")) mustEqual -2.0

      val exp2 = Expression("multiply($1,$2,$3)")
      exp2.apply(Array("","1","2","3.0")) mustEqual 6.0
      exp2.apply(Array("","-1","2","3.0")) mustEqual -6.0
    }
    "multiply a list" >> {
      val exp = Expression("multiply($0)")
      exp.apply(Array(Seq("2","3").asJava)) mustEqual 6.0
    }
    "subtract" >> {
      val exp1 = Expression("subtract($1,$2)")
      exp1.apply(Array("","2","1")) mustEqual 1.0
      exp1.apply(Array("","-1","2")) mustEqual -3.0

      val exp2 = Expression("subtract($1,$2,$3)")
      exp2.apply(Array("","1","2","3.0")) mustEqual -4.0
      exp2.apply(Array("","-1","2","3.0")) mustEqual -6.0
    }
    "subtract a list" >> {
      val exp = Expression("subtract($0)")
      exp.apply(Array(Seq("2","1").asJava)) mustEqual 1.0
    }
    "divide" >> {
      val exp1 = Expression("divide($1,$2)")
      exp1.apply(Array("","2","1")) mustEqual 2.0
      exp1.apply(Array("","-1","2")) mustEqual -0.5

      val exp2 = Expression("divide($1,$2,$3)")
      exp2.apply(Array("","1","2","3.0")) mustEqual (1.0/2/3) // 0.166666666666
      exp2.apply(Array("","-1","2","3.0")) mustEqual (-1.0/2/3) // -0.166666666666
    }
    "divide a list" >> {
      val exp = Expression("divide($0)")
      exp.apply(Array(Seq("2","1").asJava)) mustEqual 2.0
    }
    "find mean" >> {
      val exp1 = Expression("mean($1,$2,$3,$4)")
      exp1.apply(Array("","1","2","3","4")) mustEqual 2.5
    }
    "find mean of list" >> {
      val exp1 = Expression("mean($0)")
      exp1.apply(Array(Seq("1","2","3","4").asJava)) mustEqual 2.5
    }
    "find min" >> {
      val exp1 = Expression("min($1,$2,$3,$4)::int")
      exp1.apply(Array("","1","2","3","4")) mustEqual 1
    }
    "find min of list" >> {
      val exp1 = Expression("min($0)::int")
      exp1.apply(Array(Seq("1","2","3","4").asJava)) mustEqual 1
    }
    "find min of dates" >> {
      val exp1 = Expression("min($0,$1,$2)")
      exp1.apply(Array.tabulate(3)(i => new Date(i))) mustEqual new Date(0)
    }
    "find max" >> {
      val exp1 = Expression("max($1,$2,$3,$4)::int")
      exp1.apply(Array("","1","2","3","4")) mustEqual 4
    }
    "find max of list" >> {
      val exp1 = Expression("max($0)::int")
      exp1.apply(Array(Seq("1","2","3","4").asJava)) mustEqual 4
    }
    "find max of dates" >> {
      val exp1 = Expression("max($0,$1,$2)")
      exp1.apply(Array.tabulate(3)(i => new Date(i))) mustEqual new Date(2)
    }
    "allow for number formatting using printf" >> {
      val exp = Expression("printf('%.2f', divide($1,$2,$3))")
      exp.apply(Array("","-1","2","3.0")) mustEqual "-0.17"
    }
    "allow for number formatting using printf" >> {
      val exp = Expression("printf('%.2f', divide(-1, 2, 3))")
      exp.apply(Array()) mustEqual "-0.17"
    }
    "support cql buffer" >> {
      val exp = Expression("cql:buffer($1, $2)")
      val buf = exp.apply(Array(null, "POINT(1 1)", Double.box(2.0)))
      buf must not(beNull)
      buf.getClass mustEqual classOf[Polygon]
      buf.asInstanceOf[Polygon].getCentroid.getX must beCloseTo(1, 0.0001)
      buf.asInstanceOf[Polygon].getCentroid.getY must beCloseTo(1, 0.0001)
      // note: area is not particularly close as there aren't very many points in the polygon
      buf.asInstanceOf[Polygon].getArea must beCloseTo(math.Pi * 4.0, 0.2)
    }
    "pass literals through to cql functions" >> {
      val exp = Expression("cql:intersects(geometry('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'), $1)")
      exp must beAnInstanceOf[FunctionExpression]
      exp.asInstanceOf[FunctionExpression].arguments.headOption must beSome(beAnInstanceOf[Literal[_]])
      exp.apply(Array(null, "POINT(27.8 31.1)")) mustEqual true
      exp.apply(Array(null, "POINT(1 1)")) mustEqual false
    }
    "convert stringToDouble zero default" >> {
      val exp = Expression("stringToDouble($1, 0.0)")
      exp.apply(Array("", "1.2")) mustEqual 1.2
      exp.apply(Array("", "")) mustEqual 0.0
      exp.apply(Array("", null)) mustEqual 0.0
      exp.apply(Array("", "notadouble")) mustEqual 0.0
    }
    "convert stringToDouble null default" >> {
      val exp = Expression("stringToDouble($1, $2)")
      exp.apply(Array("", "1.2", null)) mustEqual 1.2
      exp.apply(Array("", "", null)) mustEqual null
      exp.apply(Array("", null, null)) mustEqual null
      exp.apply(Array("", "notadouble", null)) mustEqual null
    }
    "convert stringToInt zero default" >> {
      foreach(Seq("stringToInt", "stringToInteger")) { fn =>
        val exp = Expression(s"$fn($$1, 0)")
        exp.apply(Array("", "2")) mustEqual 2
        exp.apply(Array("", "")) mustEqual 0
        exp.apply(Array("", null)) mustEqual 0
        exp.apply(Array("", "1.2")) mustEqual 0
      }
    }
    "convert stringToInt null default" >> {
      foreach(Seq("stringToInt", "stringToInteger")) { fn =>
        val exp = Expression(s"$fn($$1, $$2)")
        exp.apply(Array("", "2", null)) mustEqual 2
        exp.apply(Array("", "", null)) mustEqual null
        exp.apply(Array("", null, null)) mustEqual null
        exp.apply(Array("", "1.2", null)) mustEqual null
      }
    }
    "convert stringToLong zero default" >> {
      val exp = Expression("stringToLong($1, 0L)")
      exp.apply(Array("", "22960000000")) mustEqual 22960000000L
      exp.apply(Array("", "")) mustEqual 0L
      exp.apply(Array("", null)) mustEqual 0L
      exp.apply(Array("", "1.2")) mustEqual 0L
    }
    "convert stringToLong null default" >> {
      val exp = Expression("stringToLong($1, $2)")
      exp.apply(Array("", "22960000000", null)) mustEqual 22960000000L
      exp.apply(Array("", "", null)) mustEqual null
      exp.apply(Array("", null, null)) mustEqual null
      exp.apply(Array("", "1.2", null)) mustEqual null
    }
    "convert stringToFloat zero default" >> {
      val exp = Expression("stringToFloat($1, 0.0f)")
      exp.apply(Array("", "1.2")) mustEqual 1.2f
      exp.apply(Array("", "")) mustEqual 0.0f
      exp.apply(Array("", null)) mustEqual 0.0f
      exp.apply(Array("", "notafloat")) mustEqual 0.0f
    }
    "convert stringToFloat zero default" >> {
      val exp = Expression("stringToFloat($1, $2)")
      exp.apply(Array("", "1.2", null)) mustEqual 1.2f
      exp.apply(Array("", "", null)) mustEqual null
      exp.apply(Array("", null, null)) mustEqual null
      exp.apply(Array("", "notafloat", null)) mustEqual null
    }
    "convert stringToBoolean false default" >> {
      val exp = Expression("stringToBoolean($1, false)")
      exp.apply(Array("", "true")) mustEqual true
      exp.apply(Array("", "")) mustEqual false
      exp.apply(Array("", null)) mustEqual false
      exp.apply(Array("", "18")) mustEqual false
    }
    "convert stringToBoolean null default" >> {
      val exp = Expression("stringToBoolean($1,$2)")
      exp.apply(Array("", "true", null)) mustEqual true
      exp.apply(Array("", "", null)) mustEqual null
      exp.apply(Array("", null, null)) mustEqual null
      exp.apply(Array("", "18", null)) mustEqual null
    }
    "return null for non-existing fields" >> {
      val fields = Seq(
        BasicField("foo", Some(Expression("$1"))),
        BasicField("bar", Some(Expression("$2"))),
        BasicField("missing", Some(Expression("$b"))),
        BasicField("found", Some(Expression("$bar")))
      )
      val metrics = ConverterMetrics.empty
      val ctx = EvaluationContext(fields, Map.empty, Map.empty, metrics, metrics.counter("s"), metrics.counter("f"))

      val result = ctx.evaluate(Array("", "5", "10"))
      result must beRight
      result.right.get mustEqual Array("5", "10", null, "10")
    }
    "create lists" >> {
      val trans = Expression("list($0, $1, $2)")
      val res = trans.apply(Array("a", "b", "c"))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.List[String]]
      res.asInstanceOf[java.util.List[String]].size() mustEqual 3
      res.asInstanceOf[java.util.List[String]].asScala mustEqual List("a", "b", "c")
    }
    "parse lists with default delimiter" >> {
      val trans = Expression("parseList('string', $0)")
      val res = trans.apply(Array("a,b,c"))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.List[String]]
      res.asInstanceOf[java.util.List[String]].size() mustEqual 3
      res.asInstanceOf[java.util.List[String]].asScala mustEqual List("a", "b", "c")
    }
    "parse lists with custom delimiter" >> {
      val trans = Expression("parseList('string', $0, '%')")
      val res = trans.apply(Array("a%b%c"))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.List[String]]
      res.asInstanceOf[java.util.List[String]].size() mustEqual 3
      res.asInstanceOf[java.util.List[String]].asScala mustEqual List("a", "b", "c")
    }
    "parse lists with numbers" >> {
      val trans = Expression("parseList('int', $0, '%')")
      val res = trans.apply(Array("1%2%3"))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.List[Int]]
      res.asInstanceOf[java.util.List[Int]].size() mustEqual 3
      res.asInstanceOf[java.util.List[Int]].asScala mustEqual List(1, 2, 3)
    }
    "parse null lists" >> {
      val trans = Expression("parseList('int', $0)")
      trans.apply(Array(null)) must beNull
      trans.apply(Array("")) mustEqual Collections.emptyList[Int]()
    }
    "parse null lists" >> {
      val trans = Expression("parseList('int', $0)")
      trans.eval(Array(null)) must beNull
      trans.eval(Array("")) mustEqual Collections.emptyList[Int]()
    }
    "throw exception for invalid list values" >> {
      val trans = Expression("parseList('int', $0, '%')")
      trans.apply(Array("1%2%a")).asInstanceOf[java.util.List[Int]] must throwAn[IllegalArgumentException]
    }
    "parse maps with default delimiter" >> {
      val trans = Expression("parseMap('String->Int', $0)")
      val res = trans.apply(Array("a->1,b->2,c->3"))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.Map[String, Int]]
      res.asInstanceOf[java.util.Map[String, Int]].size mustEqual 3
      res.asInstanceOf[java.util.Map[String, Int]].asScala mustEqual Map("a" -> 1, "b" -> 2, "c" -> 3)
    }
    "parse maps with custom delimiter" >> {
      val trans = Expression("parseMap('String->Int', $0, '%', ';')")
      val res = trans.apply(Array("a%1;b%2;c%3"))
      res must not(beNull)
      res.asInstanceOf[AnyRef] must beAnInstanceOf[java.util.Map[String, Int]]
      res.asInstanceOf[java.util.Map[String, Int]].size mustEqual 3
      res.asInstanceOf[java.util.Map[String, Int]].asScala mustEqual Map("a" -> 1, "b" -> 2, "c" -> 3)
    }
    "parse null maps" >> {
      val trans = Expression("parseMap('String->Int', $0)")
<<<<<<< HEAD
      trans.apply(Array(null)) must beNull
      trans.apply(Array("")) mustEqual Collections.emptyMap[String, Int]()
=======
      trans.eval(Array(null)) must beNull
      trans.eval(Array("")) mustEqual Collections.emptyMap[String, Int]()
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    }
    "throw exception for invalid map values" >> {
      val trans = Expression("parseMap('String->Int', $0)")
      trans.apply(Array("a->1,b->2,c->d")) must throwAn[IllegalArgumentException]
    }
    "handle default values" >> {
      val trans = Expression("withDefault($0, 'foo')")
      trans.apply(Array("bar")) mustEqual "bar"
      trans.apply(Array("")) mustEqual ""
    }
    "convert integer to boolean" >> {
      val trans = Expression("intToBoolean($0)")
      trans.apply(Array(Int.box(1))) mustEqual true
      trans.apply(Array(Int.box(0))) mustEqual false
      trans.apply(Array(Int.box(-20))) mustEqual true
      trans.apply(Array(Int.box(10000))) mustEqual true
      trans.apply(Array(Double.box(2.2))) must throwA[ClassCastException]
    }
    "strip quotes" >> {
      val trans = Expression("stripQuotes($0)")
      trans.apply(Array("'foo'")) mustEqual "foo"
      trans.apply(Array("\"foo'")) mustEqual "foo"
      trans.apply(Array("\"'foo'")) mustEqual "foo"

      // white space is preserved
      trans.apply(Array("'foo\t\t")) mustEqual "foo\t\t"
      trans.apply(Array("  foo'\"\"")) mustEqual "  foo"
    }
    "strip whitespace" >> {
      val trans = Expression("strip($0)")
      trans.apply(Array("\t   foo   \t\t\t")) mustEqual "foo"
    }
    "strip from start and end with strip" >> {
      val trans = Expression("strip($0, '\\'')")
      trans.apply(Array("'foo'")) mustEqual "foo"
      trans.apply(Array("'foo")) mustEqual "foo"
      trans.apply(Array("foo'")) mustEqual "foo"
    }
    "strip multiple chars (e.g. quotes)" >> {
      val trans = Expression("strip($0, '\\'\\\"')")
      trans.apply(Array("'foo'")) mustEqual "foo"
      trans.apply(Array("\"foo'")) mustEqual "foo"
      trans.apply(Array("\"'foo'")) mustEqual "foo"
      trans.apply(Array("'foo")) mustEqual "foo"
      trans.apply(Array("foo'\"\"")) mustEqual "foo"
    }
    "strip prefix only" >> {
      val trans = Expression("stripPrefix($0, '\\'')")
      trans.apply(Array("'foo'", "'")) mustEqual "foo'"
      trans.apply(Array("'foo", "'")) mustEqual "foo"
      trans.apply(Array("foo'", "'")) mustEqual "foo'"
      trans.apply(Array(" 'foo'", "'")) mustEqual " 'foo'"
    }
    "strip prefix with  multiple chars" >> {
      val trans = Expression("stripPrefix($0, '\\'\\\"')")
      trans.apply(Array("'foo'", "'")) mustEqual "foo'"
      trans.apply(Array("'foo", "'")) mustEqual "foo"
      trans.apply(Array("foo'", "'")) mustEqual "foo'"
      trans.apply(Array("\"'foo\"", "'")) mustEqual "foo\""
      trans.apply(Array("\"\"\"'foo'", "'")) mustEqual "foo'"
    }
    "strip suffix only" >> {
      val trans = Expression("stripSuffix($0, '\\'')")
      trans.apply(Array("'foo'")) mustEqual "'foo"
      trans.apply(Array("'foo")) mustEqual "'foo"
      trans.apply(Array("foo'")) mustEqual "foo"
    }
    "strip suffix with preserving whitespace" >> {
      val trans = Expression("stripSuffix($0, 'ab')")
      trans.apply(Array("fooab ")) mustEqual "fooab "
    }
    "strip suffix multiple chars" >> {
      val trans = Expression("stripSuffix($0, '\\'\\\"')")
      trans.apply(Array("'\"foo'")) mustEqual "'\"foo"
      trans.apply(Array("'\"foo")) mustEqual "'\"foo"
      trans.apply(Array("\"foo'")) mustEqual "\"foo"
      trans.apply(Array("'foo\"'")) mustEqual "'foo"
      trans.apply(Array("'foo\"")) mustEqual "'foo"
      trans.apply(Array("foo'\"")) mustEqual "foo"
    }
    "strip something other than quotes" >> {
      val trans = Expression("strip($0, 'X')")
      trans.apply(Array("XfooX")) mustEqual "foo"
      trans.apply(Array("Xfoo")) mustEqual "foo"
      trans.apply(Array("fooX")) mustEqual "foo"
    }
    "remove strings" >> {
      val trans = Expression("remove($0, '\\'')")
      trans.apply(Array("'foo'")) mustEqual "foo"
      trans.apply(Array("'foo")) mustEqual "foo"
      trans.apply(Array("foo'")) mustEqual "foo"
      trans.apply(Array("f'o'o'")) mustEqual "foo"
      Expression("remove($0, 'abc')").apply(Array("foabco")) mustEqual "foo"
    }
    "replace" >> {
      val trans = Expression("replace($0, '\\'', '\\\"')")
      trans.apply(Array("'foo'")) mustEqual "\"foo\""
      trans.apply(Array("'foo")) mustEqual "\"foo"
      trans.apply(Array("foo'")) mustEqual "foo\""
      trans.apply(Array("f'o'o'")) mustEqual "f\"o\"o\""
      Expression("replace($0, 'a', 'o')").apply(Array("faa")) mustEqual "foo"
    }
    "select a list item" >> {
      val exp = Expression("listItem($0,1)")
      exp.apply(Array(Seq(1000, 2000).asJava)) mustEqual 2000
    }
    "map a list" >> {
      val exp = Expression("transformListItems($0,'millisToDate($0)')")
      exp.apply(Array(Seq(1000, 2000).asJava)) mustEqual Seq(new Date(1000), new Date(2000)).asJava
    }
  }
}
