/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.commons.csv.CSVFormat
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Date}

@RunWith(classOf[JUnitRunner])
class DelimitedTextConverterTest extends Specification {

  import scala.collection.JavaConverters._

  sequential

<<<<<<< HEAD
  val data = Seq(
    """1,hello,45.0,45.0""",
    """2,world,90.0,90.0""",
    """willfail,hello""").mkString("\n")

  val conf = ConfigFactory.parseString(
    """
      | {
      |   type         = "delimited-text",
      |   format       = "DEFAULT",
      |   id-field     = "md5(string2bytes($0))",
      |   fields = [
      |     { name = "oneup",    transform = "$1" },
      |     { name = "phrase",   transform = "concat($1, $2)" },
      |     { name = "lat",      transform = "$3::double" },
      |     { name = "lon",      transform = "$4::double" },
      |     { name = "lit",      transform = "'hello'" },
      |     { name = "geom",     transform = "point($lat, $lon)" }
      |     { name = "l1",       transform = "concat($lit, $lit)" }
      |     { name = "l2",       transform = "concat($l1,  $lit)" }
      |     { name = "l3",       transform = "concat($l2,  $lit)" }
      |   ]
      | }
      """.stripMargin)

  val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))

  "DelimitedTextConverter" should {

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======

  val data = Seq(
    """1,hello,45.0,45.0""",
    """2,world,90.0,90.0""",
    """willfail,hello""").mkString("\n")

  val conf = ConfigFactory.parseString(
    """
      | {
      |   type         = "delimited-text",
      |   format       = "DEFAULT",
      |   id-field     = "md5(string2bytes($0))",
      |   fields = [
      |     { name = "oneup",    transform = "$1" },
      |     { name = "phrase",   transform = "concat($1, $2)" },
      |     { name = "lat",      transform = "$3::double" },
      |     { name = "lon",      transform = "$4::double" },
      |     { name = "lit",      transform = "'hello'" },
      |     { name = "geom",     transform = "point($lat, $lon)" }
      |     { name = "l1",       transform = "concat($lit, $lit)" }
      |     { name = "l2",       transform = "concat($l1,  $lit)" }
      |     { name = "l3",       transform = "concat($l2,  $lit)" }
      |   ]
      | }
      """.stripMargin)

  val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))

  "DelimitedTextConverter" should {

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
    "be built from a conf" >> {
      SimpleFeatureConverter(sft, conf).close() must not(throwAn[Exception])
    }

    "process some data" >> {
      val res = WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
      }
      res.size must be equalTo 2
      res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
      res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
      // handle more derived fields than input fields
      res(0).getAttribute("oneup").asInstanceOf[String] must be equalTo "1"
      // correctly identify feature IDs based on lines
<<<<<<< HEAD
      res(0).getID mustEqual "924ab432cc82d3442f94f3c4969a2b0e" // hashing.hashBytes("1,hello,45.0,45.0".getBytes(StandardCharsets.UTF_8)).toString
      res(1).getID mustEqual "cd8bf6a68220d43c9158ff101a30a99d" // hashing.hashBytes("2,world,90.0,90.0".getBytes(StandardCharsets.UTF_8)).toString
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
      val hashing = Hashing.md5()
      res(0).getID mustEqual hashing.hashBytes("1,hello,45.0,45.0".getBytes(StandardCharsets.UTF_8)).toString
      res(1).getID mustEqual hashing.hashBytes("2,world,90.0,90.0".getBytes(StandardCharsets.UTF_8)).toString
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
    }

    "handle tab delimited files" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "TDF",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "concat($1, $2)" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)
      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val stream = new ByteArrayInputStream(data.replaceAll(",", "\t").getBytes(StandardCharsets.UTF_8))
        val res = WithClose(converter.process(stream))(_.toList)
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
        res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
      }
    }

    "handle line number transform and filename global parameter correctly " >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "TDF",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "concat($1, $2)" },
          |     { name = "lineNr", transform = "lineNo()"},
          |     { name = "fn",     transform = "$filename"},
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)
      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val stream = new ByteArrayInputStream(data.replaceAll(",", "\t").getBytes(StandardCharsets.UTF_8))
        val ec = converter.createEvaluationContext(Map("filename"-> "/some/file/path/testfile.txt"))
        val res = WithClose(converter.process(stream, ec))(_.toList)
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
        res(0).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 1
        res(0).getAttribute("fn").asInstanceOf[String] must be equalTo "/some/file/path/testfile.txt"
        res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
        res(1).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 2
        res(1).getAttribute("fn").asInstanceOf[String] must be equalTo "/some/file/path/testfile.txt"
      }
    }

    "handle line number transform and filename global in id-field " >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "TDF",
          |   id-field     = "concat($filename, lineNo())",
          |   fields = [
          |     { name = "phrase", transform = "concat($1, $2)" },
          |     { name = "lineNr", transform = "lineNo()"},
          |     { name = "fn",     transform = "$filename"},
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val stream = new ByteArrayInputStream(data.replaceAll(",", "\t").getBytes(StandardCharsets.UTF_8))
        val ec = converter.createEvaluationContext(Map("filename"-> "/some/file/path/testfile.txt"))
        val res = WithClose(converter.process(stream, ec))(_.toList)
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
        res(0).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 1
        res(0).getAttribute("fn").asInstanceOf[String] must be equalTo "/some/file/path/testfile.txt"
        res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
        res(1).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 2
        res(1).getAttribute("fn").asInstanceOf[String] must be equalTo "/some/file/path/testfile.txt"
      }
    }

    "handle projecting to just the attributes in the SFT (and associated input dependencies)" >> {
      // l3 has cascading dependencies
      val subsft = SimpleFeatureTypes.createType("subsettest", "l3:String,geom:Point:srid=4326")
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res must haveLength(2)
      }
    }

    "handle horrible quoting and nested separators" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "EXCEL",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "concat($1, $2)" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val res = WithClose(converter.process(getClass.getClassLoader.getResourceAsStream("messydata.csv")))(_.toList)
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello, \"foo\""
        res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
      }
    }

    "disable quote characters" >> {
      val quoteConf = conf.withValue("options", ConfigValueFactory.fromMap(Collections.singletonMap("quote", "")))
      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, quoteConf)) { converter =>
        converter must not(beNull)
        val data = Seq(
          """1,hello",45.0,45.0""",
          """2,"world,90.0,90.0""",
          """willfail,hello""").mkString("\n")
        val stream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
        val res = WithClose(converter.process(stream))(_.toList)
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello\""
        res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2\"world"
      }
    }

    "handle records bigger than buffer size" >> {
      // set the buffer size to 16 bytes and try to write records that are bigger than the buffer size

      val sizeConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   options = {
          |       pipe-size    = 16 // 16 bytes
          |   },
          |   fields = [
          |     { name = "oneup",  transform = "$1" },
          |     { name = "phrase", transform = "concat($1, $2)" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "lit",    transform = "'hello'" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val data =
          """
            |1,hello,45.0,45.0
            |2,world,90.0,90.0
            |willfail,hello
          """.stripMargin

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
          res must not(beEmpty)
      }
    }

    "handle wkt" >> {
      val wktData =
        """
          |1,hello,Point(46.0 45.0)
          |2,world,Point(90.0 90.0)
        """.stripMargin

      val wktConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "oneup",    transform = "$1" },
          |     { name = "phrase",   transform = "concat($1, $2)" },
          |     { name = "geom",     transform = "geometry($3)" }
          |   ]
          | }
        """.stripMargin)

      val wktSft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(wktSft, wktConf)) { converter =>
        converter must not(beNull)

        val res = WithClose(converter.process(new ByteArrayInputStream(wktData.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.length mustEqual 2

        val geoFac = new GeometryFactory()
        res(0).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(46, 45))
        res(1).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(90, 90))
      }
    }

    "skip header lines" >> {
       val conf =
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   options = {
          |       skip-lines = SKIP
          |   },
          |   fields = [
          |     { name = "oneup",    transform = "$1" },
          |     { name = "phrase",   transform = "concat($1, $2)" },
          |     { name = "geom",     transform = "geometry($3)" }
          |   ]
          | }
        """.stripMargin
      val wktSft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))

      "csv parser failblog or misunderstanding test" >> {
        val format = CSVFormat.DEFAULT.withSkipHeaderRecord(true).withIgnoreEmptyLines(true)
        val trueData =
          """
            |num,msg,geom
            |1,hello,Point(46.0 45.0)
            |2,world,Point(90.0 90.0)
          """.stripMargin
        val sz = format.parse(new InputStreamReader(new ByteArrayInputStream(trueData.getBytes(StandardCharsets.UTF_8)))).iterator().asScala.toList.size

        // prove that skipHeader and empty lines doesn't work (at least as I think) and that we are safe to
        // consume the header record and empty lines as part of our config
        sz mustEqual 4
      }
      "with single line header" >> {
        val trueConf = ConfigFactory.parseString(conf.replaceAllLiterally("SKIP", "1"))
        val trueData =
          """num,msg,geom
            |1,hello,Point(46.0 45.0)
            |2,world,Point(90.0 90.0)""".stripMargin
        WithClose(SimpleFeatureConverter(wktSft, trueConf)) { converter =>
          converter must not(beNull)

          val ec = converter.createEvaluationContext()
          val stream = new ByteArrayInputStream(trueData.getBytes(StandardCharsets.UTF_8))
          val res = WithClose(converter.process(stream, ec))(_.toList)
          res.length mustEqual 2

          ec.line mustEqual 3
          ec.success.getCount mustEqual 2
          ec.failure.getCount mustEqual 0

          val geoFac = new GeometryFactory()
          res(0).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(46, 45))
          res(1).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(90, 90))
        }
      }

      "with header set to 0" >> {
        val falseConf = ConfigFactory.parseString(conf.replaceAllLiterally("SKIP", "0"))
        val falseData =
          """1,hello,Point(46.0 45.0)
            |2,world,Point(90.0 90.0)""".stripMargin
        WithClose(SimpleFeatureConverter(wktSft, falseConf)) { converter =>
          converter must not(beNull)

          val ec = converter.createEvaluationContext()
          val res = WithClose(converter.process(new ByteArrayInputStream(falseData.getBytes(StandardCharsets.UTF_8)), ec))(_.toList)
          res.length mustEqual 2

          ec.line mustEqual 2
          ec.success.getCount mustEqual 2
          ec.failure.getCount mustEqual 0

          val geoFac = new GeometryFactory()
          res(0).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(46, 45))
          res(1).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(90, 90))
        }
      }
      "with header set to 3" >> {
        val falseConf = ConfigFactory.parseString(conf.replaceAllLiterally("SKIP", "3"))
        val falseData =
          """num,msg,geom
            |some other garbage
            |that somebody placed in my file maybe as a comment
            |1,hello,Point(46.0 45.0)
            |2,world,Point(90.0 90.0)""".stripMargin
        WithClose(SimpleFeatureConverter(wktSft, falseConf)) { converter =>
          converter must not(beNull)

          val ec = converter.createEvaluationContext()
          val res = WithClose(converter.process(new ByteArrayInputStream(falseData.getBytes(StandardCharsets.UTF_8)), ec))(_.toList)
          res.length mustEqual 2

          ec.line mustEqual 5
          ec.success.getCount mustEqual 2
          ec.failure.getCount mustEqual 0

          val geoFac = new GeometryFactory()
          res(0).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(46, 45))
          res(1).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(90, 90))
        }
      }
    }

    "handle user data" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   user-data    = {
          |     my.first.key   = "$1::int",
          |     my.second.key  = "$2",
          |     "my.third.key" = "$concat"
          |   }
          |   fields = [
          |     { name = "concat", transform = "concat($1, $2)" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)
      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.size must be equalTo 2
        res(0).getUserData.get("my.first.key") mustEqual 1
        res(0).getUserData.get("my.second.key") mustEqual "hello"
        res(0).getUserData.get("my.third.key") mustEqual "1hello"
        res(1).getUserData.get("my.first.key") mustEqual 2
        res(1).getUserData.get("my.second.key") mustEqual "world"
        res(1).getUserData.get("my.third.key") mustEqual "2world"
      }
    }

    "handle single quotes" >> {

      val data =
        """
          |'1','hello','45.0','45.0'
          |'2','world','90.0','90.0'
        """.stripMargin

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "$2" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          |   options = {
          |      quote = "'"
          |   }
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "hello"
      }
    }

    "handle custom escape" >> {

      val data =
        """
          |'1','he#'llo','45.0','45.0'
          |'2','world','90.0','90.0'
        """.stripMargin

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "$2" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          |   options = {
          |      quote = "'"
          |      escape = "#"
          |   }
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "he'llo"
      }
    }

    "handle custom delimiter" >> {

      val data =
        """
          |1;hello;45.0;45.0
          |2;world;90.0;90.0
        """.stripMargin

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "$2" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          |   options = {
          |      delimiter = ";"
          |   }
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res must haveLength(2)
        res(0).getAttribute("phrase") mustEqual "hello"
        res(1).getAttribute("phrase") mustEqual "world"
      }
    }

    "throw error on escape length > 1" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "$2" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          |   options = {
          |      quote = "'"
          |      escape = "##"
          |   }
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      SimpleFeatureConverter(sft, conf) must throwAn[IllegalArgumentException]
    }

    "throw error on quote length > 1" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "$2" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          |   options = {
          |      quote = "''"
          |   }
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      SimpleFeatureConverter(sft, conf) must throwAn[IllegalArgumentException]
    }

    "handle out-of-order attributes" >> {

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "$fid",
          |   user-data    = {
          |     my.first.key  = "$fid",
          |     my.second.key = "$2",
          |     my.third.key  = "$concat"
          |   }
          |   fields = [
          |     { name = "concat", transform = "concat($fid, $hello)" },
          |     { name = "hello",  transform = "concat('hello ', $fid)" },
          |     { name = "fid2",   transform = "$fid" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |     { name = "fid",    transform = "$1" },
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType("test", "hello:String,*geom:Point:srid=4326")

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val converted = WithClose(converter.process(new ByteArrayInputStream("myfid,foo,45.0,55.0".getBytes(StandardCharsets.UTF_8))))(_.toList)
        converted must haveLength(1)
        converted.head.getID mustEqual "myfid"
        converted.head.getAttributes.asScala mustEqual Seq("hello myfid", WKTUtils.read("POINT(45 55)"))
        converted.head.getUserData.asScala mustEqual
            Map("my.first.key"              -> "myfid"
              , "my.second.key"             -> "foo"
              , Hints.USE_PROVIDED_FID      -> true
              , "my.third.key"              -> "myfidhello myfid")
      }
    }

    "detect circular dependencies" >> {
      val sft = SimpleFeatureTypes.createType("test", "hello:String,*geom:Point:srid=4326")

      val conf1 = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "$1",
          |   fields = [
          |     { name = "hello",  transform = "concat('hello ', $hello)" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)

      val conf2 = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "$1",
          |   fields = [
          |     { name = "goodbye", transform = "concat('goodbye ', $hello)" },
          |     { name = "hello",   transform = "concat('hello ', $goodbye)" },
          |     { name = "lat",     transform = "$3::double" },
          |     { name = "lon",     transform = "$4::double" },
          |     { name = "geom",    transform = "point($lat, $lon)" }
          |   ]
          | }
        """.stripMargin)

      val conf3 = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "$1",
          |   fields = [
          |     { name = "nihao", transform = "concat('ni hao ', $hello)" },
          |     { name = "hola",  transform = "concat('hola ', $nihao)" },
          |     { name = "hello", transform = "concat('hello ', $hola)" },
          |     { name = "geom",  transform = "point($3::double, $$4::double)" }
          |   ]
          | }
        """.stripMargin)

      foreach(Seq(conf1, conf2, conf3)) { conf =>
        SimpleFeatureConverter(sft, conf) must throwAn[IllegalArgumentException]
      }
    }

    "handle multiline csv escape" >> {

      val data =
        """
          |'1','he
          |llo','45.0','45.0'
          |'2','world','90.0','90.0'
        """.stripMargin

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "$2" },
          |     { name = "lat",    transform = "$3::double" },
          |     { name = "lon",    transform = "$4::double" },
          |     { name = "geom",   transform = "point($lat, $lon)" }
          |   ]
          |   options = {
          |      quote = "'"
          |      escape = "#"
          |   }
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "he\nllo"
      }
    }

    "handle skip lines without trailing newline" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "delimited-text",
          |   format       = "DEFAULT",
          |   id-field     = "md5(string2bytes($0))",
          |   fields = [
          |     { name = "phrase", transform = "$2" },
          |     { name = "geom",   transform = "point($3::double, $4::double)" }
          |   ]
          |   options = {
          |     skip-lines = 0
          |   }
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType("test", "phrase:String,*geom:Point:srid=4326")
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val data = """1,hello,45.0,45.0"""

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res must haveSize(1)
        res(0).getAttribute("phrase") mustEqual "hello"
        res(0).getAttribute("geom").toString mustEqual "POINT (45 45)"
      }
    }

    "infer a converter from input data" >> {
      import scala.collection.JavaConverters._

      val data =
        """
          |num,word,lat,lon
          |1,hello,45.0,45.0
          |2,world,90.0,90.0
        """.stripMargin

      val factory = new DelimitedTextConverterFactory()
      val inferred = factory.infer(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), None, Map.empty[String, AnyRef])
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[Integer], classOf[String], classOf[java.lang.Double], classOf[java.lang.Double], classOf[Point])

      val converter = factory.apply(sft, config)
      converter must beSome

      val features = converter.get.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))).toList
      converter.get.close()
      features must haveLength(2)
      features(0).getAttributes.asScala mustEqual Seq(1, "hello", 45f, 45f, WKTUtils.read("POINT (45 45)"))
      features(1).getAttributes.asScala mustEqual Seq(2, "world", 90f, 90f, WKTUtils.read("POINT (90 90)"))
    }

    "infer a string types from null inputs" >> {
      import scala.collection.JavaConverters._

      val data =
        """
          |num,word,lat,lon
          |1,,45.0,45.0
        """.stripMargin

      val factory = new DelimitedTextConverterFactory()
      val inferred = factory.infer(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), None, Map.empty[String, AnyRef])
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[Integer], classOf[String], classOf[java.lang.Double], classOf[java.lang.Double], classOf[Point])

      val converter = factory.apply(sft, config)
      converter must beSome

      val features = converter.get.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))).toList
      converter.get.close()
      features must haveLength(1)
      features.head.getAttributes.asScala mustEqual Seq(1, "", 45f, 45f, WKTUtils.read("POINT (45 45)"))
    }

    "ingest magic files" >> {
      val data =
        """id,name:String,age:Int,*geom:Point:srid=4326
          |fid-0,name0,0,POINT(40 50)
          |fid-1,name1,1,POINT(41 51)""".stripMargin
      val is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
      val features = DelimitedTextConverter.magicParsing("foo", is).toList
      features must haveLength(2)
      foreach(0 to 1) { i =>
        features(i).getID mustEqual s"fid-$i"
        features(i).getAttributeCount mustEqual 3
        features(i).getAttribute(0) mustEqual s"name$i"
        features(i).getAttribute(1) mustEqual i
        features(i).getAttribute(2) mustEqual WKTUtils.read(s"POINT(4$i 5$i)")
      }
    }

    "type infer and ingest magic files" >> {
      import scala.collection.JavaConverters._

      val data =
        """id,fid:Integer,name:String,age:Integer,lastseen:Date,friends:List[String],"talents:Map[String,Integer]",*geom:Point:srid=4326
          |23623,23623,Harry,20,2015-05-06T00:00:00.000Z,"Will,Mark,Suzan","patronus->10,expelliarmus->9",POINT (-100.2365 23)
          |26236,26236,Hermione,25,2015-06-07T00:00:00.000Z,"Edward,Bill,Harry",accio->10,POINT (40.232 -53.2356)
          |3233,3233,Severus,30,2015-10-23T00:00:00.000Z,"Tom,Riddle,Voldemort",potions->10,POINT (3 -62.23)
          |""".stripMargin

      val factory = new DelimitedTextConverterFactory()
      val inferred = factory.infer(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), None, Map.empty[String, AnyRef])
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get

      SimpleFeatureTypes.encodeType(sft) mustEqual
          "fid:Integer,name:String,age:Integer,lastseen:Date,friends:List[String],talents:Map[String,Integer],*geom:Point:srid=4326"

      val converter = factory.apply(sft, config)
      converter must beSome

      val features = converter.get.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))).toList
      converter.get.close()

      features must haveLength(3)
      features.map(_.getID) mustEqual Seq("23623", "26236", "3233")

      features.map(_.getAttributes.asScala) mustEqual Seq(
        Seq(23623, "Harry", 20, FastConverter.convert("2015-05-06T00:00:00.000Z", classOf[Date]), Seq("Will", "Mark", "Suzan").asJava, Map("patronus" -> 10, "expelliarmus" -> 9).asJava, WKTUtils.read("POINT (-100.2365 23)")),
        Seq(26236, "Hermione", 25, FastConverter.convert("2015-06-07T00:00:00.000Z", classOf[Date]), Seq("Edward", "Bill", "Harry").asJava, Map("accio" -> 10).asJava, WKTUtils.read("POINT (40.232 -53.2356)")),
        Seq(3233, "Severus", 30, FastConverter.convert("2015-10-23T00:00:00.000Z", classOf[Date]), Seq("Tom", "Riddle", "Voldemort").asJava, Map("potions" -> 10).asJava, WKTUtils.read("POINT (3 -62.23)"))
      )
    }
  }
}
