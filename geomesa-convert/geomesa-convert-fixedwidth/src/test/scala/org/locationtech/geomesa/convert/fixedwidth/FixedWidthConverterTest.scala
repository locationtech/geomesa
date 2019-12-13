/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.fixedwidth

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.locationtech.jts.geom.{Coordinate, Point}
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FixedWidthConverterTest extends Specification {

  sequential

  "FixedWidthConverter" >> {

    val data =
      """
        |14555
        |16565
      """.stripMargin

    "process fixed with data without validating" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type      = "fixed-width"
          |   id-field  = "uuid()"
          |   options {
          |     validating = false
          |   }
          |   fields = [
          |     { name = "lat",  transform = "$0::double", start = 1, width = 2 },
          |     { name = "lon",  transform = "$0::double", start = 3, width = 2 },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.size must be equalTo 2
        res(0).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(55.0, 45.0)
        res(1).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(65.0, 65.0)
      }
    }

    "set null values on out of order converter components until GEOMESA-1833 is completed" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type      = "fixed-width"
          |   id-field  = "uuid()"
          |   options {
          |     validating = false
          |   }
          |   fields = [
          |     { name = "anotherLat", transform = "$lat" },
          |     { name = "lat",  transform = "$0::double", start = 1, width = 2 },
          |     { name = "lon",  transform = "$0::double", start = 3, width = 2 },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.size must be equalTo 2
        res(0).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(55.0, 45.0)
        res(1).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(65.0, 65.0)
        res(1).getAttribute("anotherLat").asInstanceOf[Double] must be equalTo 65.0D
      }
    }

    "process with validation on" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type      = "fixed-width"
          |   id-field  = "uuid()"
          |   fields = [
          |     { name = "lat",  transform = "$0::double", start = 1, width = 2 },
          |     { name = "lon",  transform = "$0::double", start = 3, width = 2 },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.size must be equalTo 0
      }
    }

    "process user data" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type      = "fixed-width"
          |   id-field  = "uuid()"
          |   options {
          |     validating = false
          |   }
          |   user-data = {
          |     my.user.key = "$lat"
          |   }
          |   fields = [
          |     { name = "lat",  transform = "$0::double", start = 1, width = 2 },
          |     { name = "lon",  transform = "$0::double", start = 3, width = 2 },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res.size must be equalTo 2
        res(0).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(55.0, 45.0)
        res(1).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(65.0, 65.0)
        res(0).getUserData.get("my.user.key") mustEqual 45.0
        res(1).getUserData.get("my.user.key") mustEqual 65.0
      }
    }
  }
}
