/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NewLinesTest extends Specification {

  sequential

  "NewLinesTest" should {

    val conf = ConfigFactory.parseString(
      """
        | {
        |   type         = "delimited-text",
        |   format       = "DEFAULT",
        |   id-field     = "uuid()",
        |   fields = [
        |     { name = "lat",      transform = "$1::double" },
        |     { name = "lon",      transform = "$2::double" },
        |     { name = "geom",     transform = "point($lat, $lon)" }
        |   ]
        | }
      """.stripMargin)

    val sft = SimpleFeatureTypes.createType(ConfigFactory.parseString(
      """
        |{
        |  type-name = "newlinetest"
        |  attributes = [
        |    { name = "lat",      type = "Double", index = false },
        |    { name = "lon",      type = "Double", index = false },
        |    { name = "geom",     type = "Point",  index = true, srid = 4326, default = true }
        |  ]
        |}
      """.stripMargin
    ))

    "process trailing newline" >> {
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val data = "45.0,45.0\n55.0,55.0\n"

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res must haveLength(2)
      }
    }

    "process middle of data newline" >> {
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val data = "45.0,45.0\n\n55.0,55.0\n"

        val res = WithClose(converter.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))))(_.toList)
        res must haveLength(2)
      }
    }
  }
}