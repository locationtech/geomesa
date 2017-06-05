/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
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
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      val data = "45.0,45.0\n55.0,55.0\n".split("\n", -1)
      data.length mustEqual 3

      val res = converter.processInput(data.toIterator)
      res.length mustEqual 2
      converter.close()
      success
    }

    "process middle of data newline" >> {
      val converter = SimpleFeatureConverters.build[String](sft, conf)
      val data = "45.0,45.0\n\n55.0,55.0\n".split("\n", -1)
      data.length mustEqual 4

      val res = converter.processInput(data.toIterator)
      res.length mustEqual 2
      converter.close()
      success
    }
  }
}