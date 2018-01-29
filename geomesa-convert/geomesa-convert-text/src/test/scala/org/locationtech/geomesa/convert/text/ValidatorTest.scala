/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import java.io.{ByteArrayInputStream, IOException}
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ValidatorTest extends Specification {

  sequential

  val baseConf = ConfigFactory.parseString(
    """
      | {
      |   type         = "delimited-text",
      |   format       = "DEFAULT",
      |   id-field     = "uuid()",
      |   fields = [
      |     { name = "dtg",      transform = "date('yyyyMMdd', $1)" }
      |     { name = "geom",     transform = "point($2)" }
      |   ]
      | }
    """.stripMargin)

  val sft = SimpleFeatureTypes.createType(ConfigFactory.parseString(
    """
      |{
      |  type-name = "test"
      |  attributes = [
      |    { name = "dtg",      type = "Date" },
      |    { name = "geom",     type = "Point",  index = true, srid = 4326, default = true }
      |  ]
      |}
    """.stripMargin
  ))
  def is(str: String) = new ByteArrayInputStream(str.getBytes)

  "StandardOptions" should {
    "reject invalid parse mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "foobar" } """))
      SimpleFeatureConverters.build[String](sft, conf) must throwA[IllegalArgumentException]
    }

    "reject invalid validation mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.validation-mode = "foobar" } """))
      SimpleFeatureConverters.build[String](sft, conf) must throwA[IllegalArgumentException]
    }
  }

  "Converters" should {
    "skip empty dtg with default mode" >> {
      val conf = baseConf
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)

      converter.process(is("20160101,Point(2 2)")).toList.size mustEqual 1
      converter.process(is(
        """20160101,Point(2 2)
          |"20160102",Point(2 2)""".stripMargin)).toList.size mustEqual 2
      converter.process(is(
        """20160101,Point(2 2)
          |"",Point(2 2)""".stripMargin)).toList.size mustEqual 1
    }

    "throw exception with parse mode raise-errors in incremental mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.validation-mode = raise-errors } """))
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)

      converter.process(is(
        """20160101,Point(2 2)
          |"20160102",Point(2 2)""".stripMargin)).toList.size mustEqual 2
      converter.process(is(
        """20160101,Point(2 2)
          |"",Point(2 2)""".stripMargin)).toList should throwA[IOException]
    }

    "throw exception with parse mode raise-errors in batch mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.validation-mode = raise-errors } """))
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)

      converter.process(is(
        """20160101,Point(2 2)
          |"20160102",Point(2 2)""".stripMargin)).toList.size mustEqual 2
      converter.process(is(
        """20160101,Point(2 2)
          |"",Point(2 2)""".stripMargin)).toList should throwA[IOException]
    }
  }

  "ZIndexValidator" should {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
    "raise errors on bad dates" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.validation-mode = raise-errors, options.validators = ["z-index"] } """))
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)

      val format = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneOffset.UTC)
      val tooYoung = BinnedTime.ZMinDate.minusDays(1).format(format)
      val tooOld = BinnedTime.maxDate(TimePeriod.Week).plusDays(1).format(format)
      converter.process(is("20371231,Point(2 2)")).toList.size mustEqual 1
      converter.process(is("20371231,Point(2 2)")).next().point.getX mustEqual 2.0
      converter.process(is("20371231,Point(2 2)")).next().point.getY mustEqual 2.0
      converter.process(is(s"$tooYoung,Point(2 2)")) must throwAn[IOException]
      converter.process(is(s"$tooOld,Point(2 2)")) must throwAn[IOException]
    }

    "skip records with bad dates" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.validation-mode = skip-bad-records, options.validators = ["z-index"] } """))
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)

      val format = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneOffset.UTC)
      val tooOld = BinnedTime.maxDate(TimePeriod.Week).plusDays(1).format(format)
      converter.process(is("20371231,Point(2 2)")).toList.size mustEqual 1
      converter.process(is(s"$tooOld,Point(2 2)")).toList.size mustEqual 0
    }

    "raise errors on bad geo" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.validation-mode = raise-errors, options.validators = ["z-index"] } """))
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)

      converter.process(is("20120101,Point(2 2)")).toList.size mustEqual 1
      converter.process(is("20120101,Point(2 2)")).next().point.getX mustEqual 2.0
      converter.process(is("20120101,Point(2 2)")).next().point.getY mustEqual 2.0
      converter.process(is("20120101,Point(200 200)")) must throwAn[IOException]
    }

    "skip records with bad geo" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.validation-mode = skip-bad-records, options.validators = ["z-index"] } """))
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)

      converter.process(is("20120101,Point(2 2)")).toList.size mustEqual 1
      converter.process(is("20120101,Point(2 2)")).next().point.getX mustEqual 2.0
      converter.process(is("20120101,Point(2 2)")).next().point.getY mustEqual 2.0
      converter.process(is("20120101,Point(200 200)")).toList.size mustEqual 0
    }

    "skip bad geo records in batch mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.validation-mode = skip-bad-records, options.validators = ["z-index"] } """))
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      converter must not(beNull)

      val res = converter.process(is(
        """20120101,Point(2 2)
          |20120102,Point(200 200)
          |20120103,Point(3 3)
          |20120104,Point(-181 -181)
          |20120105,Point(4 4)
        """.stripMargin)).toList
      res.size mustEqual 3
      res.sortBy(_.point.getY)
      res(0).point.getY mustEqual 2.0
      res(1).point.getY mustEqual 3.0
      res(2).point.getY mustEqual 4.0

    }
  }
}
