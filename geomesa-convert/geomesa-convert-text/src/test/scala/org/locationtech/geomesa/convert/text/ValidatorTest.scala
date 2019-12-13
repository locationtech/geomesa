/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
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
      SimpleFeatureConverter(sft, conf) must throwAn[IllegalArgumentException]
    }

    "reject invalid validation mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.error-mode = "foobar" } """))
      SimpleFeatureConverter(sft, conf) must throwAn[IllegalArgumentException]
    }
  }

  "Converters" should {
    "skip empty dtg with default mode" >> {
      val conf = baseConf
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)
  
        WithClose(converter.process(is("20160101,Point(2 2)")))(_.toList) must haveLength(1)
        WithClose(converter.process(is(
          """20160101,Point(2 2)
            |"20160102",Point(2 2)""".stripMargin)))(_.toList) must haveLength(2)
            WithClose(converter.process(is(
          """20160101,Point(2 2)
            |"",Point(2 2)""".stripMargin)))(_.toList) must haveLength(1)
      }
    }

    "throw exception with parse mode raise-errors in incremental mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.error-mode = raise-errors } """))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        WithClose(converter.process(is(
          """20160101,Point(2 2)
            |"20160102",Point(2 2)""".stripMargin)))(_.toList) must haveLength(2)
        WithClose(converter.process(is(
          """20160101,Point(2 2)
            |"",Point(2 2)""".stripMargin)))(_.toList) should throwA[IOException]
      }
    }

    "throw exception with parse mode raise-errors in batch mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.error-mode = raise-errors } """))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        WithClose(converter.process(is(
          """20160101,Point(2 2)
            |"20160102",Point(2 2)""".stripMargin)))(_.toList) must haveLength(2)
        WithClose(converter.process(is(
          """20160101,Point(2 2)
            |"",Point(2 2)""".stripMargin)))(_.toList) should throwA[IOException]
      }
    }
  }

  "ZIndexValidator" should {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
    "raise errors on bad dates" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.error-mode = raise-errors, options.validators = ["index"] } """))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val format = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneOffset.UTC)
        val tooYoung = BinnedTime.ZMinDate.minusDays(1).format(format)
        val tooOld = BinnedTime.maxDate(TimePeriod.Week).plusDays(1).format(format)
        val res = WithClose(converter.process(is("20371231,Point(2 2)")))(_.toList)
        res must haveLength(1)
        res.head.point.getX mustEqual 2.0
        res.head.point.getY mustEqual 2.0
        converter.process(is(s"$tooYoung,Point(2 2)")) must throwAn[IOException]
        converter.process(is(s"$tooOld,Point(2 2)")) must throwAn[IOException]
      }
    }

    "skip records with bad dates" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.error-mode = skip-bad-records, options.validators = ["index"] } """))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val format = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneOffset.UTC)
        val tooOld = BinnedTime.maxDate(TimePeriod.Week).plusDays(1).format(format)
        WithClose(converter.process(is("20371231,Point(2 2)")))(_.toList) must haveLength(1)
        WithClose(converter.process(is(s"$tooOld,Point(2 2)")))(_.toList) must haveLength(0)
      }
    }

    "raise errors on bad geo" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.error-mode = raise-errors, options.validators = ["index"] } """))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val res = WithClose(converter.process(is("20120101,Point(2 2)")))(_.toList)
        res must haveLength(1)
        res.head.point.getX mustEqual 2.0
        res.head.point.getY mustEqual 2.0
        converter.process(is("20120101,Point(200 200)")) must throwAn[IOException]
      }
    }

    "skip records with bad geo" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.error-mode = skip-bad-records, options.validators = ["index"] } """))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val res = WithClose(converter.process(is("20120101,Point(2 2)")))(_.toList)
        res must haveLength(1)
        res.head.point.getX mustEqual 2.0
        res.head.point.getY mustEqual 2.0
        WithClose(converter.process(is("20120101,Point(200 200)")))(_.toList) must haveLength(0)
      }
    }

    "skip bad geo records in batch mode" >> {
      val conf = baseConf.withFallback(ConfigFactory.parseString(
        """ { options.parse-mode = "batch", options.error-mode = skip-bad-records, options.validators = ["index"] } """))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val res = converter.process(is(
          """20120101,Point(2 2)
            |20120102,Point(200 200)
            |20120103,Point(3 3)
            |20120104,Point(-181 -181)
            |20120105,Point(4 4)
          """.stripMargin)).toList
        res must haveLength(3)
        res.sortBy(_.point.getY)
        res(0).point.getY mustEqual 2.0
        res(1).point.getY mustEqual 3.0
        res(2).point.getY mustEqual 4.0
      }
    }
  }
}
