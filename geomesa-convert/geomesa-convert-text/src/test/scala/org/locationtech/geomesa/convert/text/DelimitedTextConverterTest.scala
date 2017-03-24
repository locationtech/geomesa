/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import java.io.{InputStreamReader, ByteArrayInputStream}
import java.nio.charset.StandardCharsets

import com.google.common.io.Resources
import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.commons.csv.CSVFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert.Transformers.DefaultCounter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DelimitedTextConverterTest extends Specification {

  sequential

  "DelimitedTextConverter" should {

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
    "be built from a conf" >> {
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not beNull

      val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0)).toList
      converter.close()

      "and process some data" >> {
        res.size must be equalTo 2
        res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
        res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
      }

      "handle more derived fields than input fields" >> {
        res(0).getAttribute("oneup").asInstanceOf[String] must be equalTo "1"
      }
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
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not beNull
      val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0).map(_.replaceAll(",", "\t"))).toList
      converter.close()
      res.size must be equalTo 2
      res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
      res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
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
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not beNull
      val input = data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0).map(_.replaceAll(",", "\t"))
      val ec = converter.createEvaluationContext(Map("filename"-> "/some/file/path/testfile.txt"))
      val res = converter.processInput(input, ec).toList
      converter.close()
      res.size must be equalTo 2
      res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
      res(0).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 1
      res(0).getAttribute("fn").asInstanceOf[String] must be equalTo "/some/file/path/testfile.txt"
      res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
      res(1).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 2
      res(1).getAttribute("fn").asInstanceOf[String] must be equalTo "/some/file/path/testfile.txt"
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
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not beNull
      val input = data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0).map(_.replaceAll(",", "\t"))
      val ec = converter.createEvaluationContext(Map("filename"-> "/some/file/path/testfile.txt"))
      val res = converter.processInput(input, ec).toList
      converter.close()
      res.size must be equalTo 2
      res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello"
      res(0).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 1
      res(0).getAttribute("fn").asInstanceOf[String] must be equalTo "/some/file/path/testfile.txt"
      res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
      res(1).getAttribute("lineNr").asInstanceOf[Long] must be equalTo 2
      res(1).getAttribute("fn").asInstanceOf[String] must be equalTo "/some/file/path/testfile.txt"
    }

    "handle projecting to just the attributes in the SFT (and associated input dependencies)" >> {
      // l3 has cascading dependencies
      val subsft = SimpleFeatureTypes.createType("subsettest", "l3:String,geom:Point:srid=4326")
      val conv = SimpleFeatureConverters.build[String](subsft, conf)
      val res = conv.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0)).toList
      conv.close()

      res.length must be equalTo 2
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

      import scala.collection.JavaConversions._
      val data = Resources.readLines(Resources.getResource("messydata.csv"), StandardCharsets.UTF_8)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not beNull
      val res = converter.processInput(data.iterator()).toList
      converter.close()
      res.size must be equalTo 2
      res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "1hello, \"foo\""
      res(1).getAttribute("phrase").asInstanceOf[String] must be equalTo "2world"
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

      val converter = SimpleFeatureConverters.build[String](sft, sizeConf)
      converter.asInstanceOf[DelimitedTextConverter]
      val data =
        """
          |1,hello,45.0,45.0
          |2,world,90.0,90.0
          |willfail,hello
        """.stripMargin

      val nonEmptyData = data.split("\n").toIterator.filterNot(s => "^\\s*$".r.findFirstIn(s).size > 0)
      val res = converter.processInput(nonEmptyData).toList
      converter.close()

      res.size must be greaterThan 0
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
      val converter = SimpleFeatureConverters.build[String](wktSft, wktConf)

      val res = converter.processInput(wktData.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0)).toList
      res.length mustEqual 2
      converter.close()

      val geoFac = new GeometryFactory()
      res(0).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(46, 45))
      res(1).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(90, 90))
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

        import scala.collection.JavaConversions._
        val sz = format.parse(new InputStreamReader(new ByteArrayInputStream(trueData.getBytes))).iterator().toList.size

        // prove that skipHeader and empty lines doesn't work (at least as I think) and that we are safe to
        // consume the header record and empty lines as part of our config
        sz mustEqual 4
      }
      "with single line header" >> {
        val trueConf = ConfigFactory.parseString(conf.replaceAllLiterally("SKIP", "1"))
        val trueData =
          """
            |num,msg,geom
            |1,hello,Point(46.0 45.0)
            |2,world,Point(90.0 90.0)
          """.stripMargin
        val converter = SimpleFeatureConverters.build[String](wktSft, trueConf)

        val counter = new DefaultCounter
        val ec = converter.createEvaluationContext(counter = counter)
        val res = converter.processInput(trueData.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0), ec).toList
        res.length mustEqual 2
        converter.close()

        counter.getLineCount mustEqual 3
        counter.getSuccess mustEqual 2
        counter.getFailure mustEqual 0

        val geoFac = new GeometryFactory()
        res(0).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(46, 45))
        res(1).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(90, 90))
      }

      "with header set to 0" >> {
        val falseConf = ConfigFactory.parseString(conf.replaceAllLiterally("SKIP", "0"))
        val falseData =
          """
            |1,hello,Point(46.0 45.0)
            |2,world,Point(90.0 90.0)
          """.stripMargin
        val converter = SimpleFeatureConverters.build[String](wktSft, falseConf)

        val counter = new DefaultCounter
        val ec = converter.createEvaluationContext(counter = counter)
        val res = converter.processInput(falseData.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0), ec).toList
        res.length mustEqual 2
        converter.close()

        counter.getLineCount mustEqual 2
        counter.getSuccess mustEqual 2
        counter.getFailure mustEqual 0

        val geoFac = new GeometryFactory()
        res(0).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(46, 45))
        res(1).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(90, 90))
      }
      "with header set to 3" >> {
        val falseConf = ConfigFactory.parseString(conf.replaceAllLiterally("SKIP", "3"))
        val falseData =
          """
            |num,msg,geom
            |some other garbage
            |that somebody placed in my file maybe as a comment
            |1,hello,Point(46.0 45.0)
            |2,world,Point(90.0 90.0)
          """.stripMargin
        val converter = SimpleFeatureConverters.build[String](wktSft, falseConf)

        val counter = new DefaultCounter
        val ec = converter.createEvaluationContext(counter = counter)
        val res = converter.processInput(falseData.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0), ec).toList
        res.length mustEqual 2
        converter.close()

        counter.getLineCount mustEqual 5
        counter.getSuccess mustEqual 2
        counter.getFailure mustEqual 0

        val geoFac = new GeometryFactory()
        res(0).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(46, 45))
        res(1).getDefaultGeometry mustEqual geoFac.createPoint(new Coordinate(90, 90))
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
          |     my.first.key  = "$1::int",
          |     my.second.key = "$2",
          |     my.third.key  = "$concat"
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
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)
      val res = converter.processInput(data.split("\n").toIterator).toList
      converter.close()
      res.size must be equalTo 2
      res(0).getUserData.get("my.first.key") mustEqual 1
      res(0).getUserData.get("my.second.key") mustEqual "hello"
      res(0).getUserData.get("my.third.key") mustEqual "1hello"
      res(1).getUserData.get("my.first.key") mustEqual 2
      res(1).getUserData.get("my.second.key") mustEqual "world"
      res(1).getUserData.get("my.third.key") mustEqual "2world"
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
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)
      val res = converter.processInput(data.split("\n").toIterator).toList
      converter.close()
      "must have size 2 " >> { res.size must be equalTo 2 }
      "first string must be 'hello'" >> { res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "hello" }
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
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)
      val res = converter.processInput(data.split("\n").toIterator).toList
      converter.close()
      "must have size 2 " >> { res.size must be equalTo 2 }
      "first string must be 'hello'" >> { res(0).getAttribute("phrase").asInstanceOf[String] must be equalTo "he'llo" }
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
      SimpleFeatureConverters.build[String](sft, conf) must throwAn[IllegalArgumentException]
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
      SimpleFeatureConverters.build[String](sft, conf) must throwAn[IllegalArgumentException]
    }

  }
}
