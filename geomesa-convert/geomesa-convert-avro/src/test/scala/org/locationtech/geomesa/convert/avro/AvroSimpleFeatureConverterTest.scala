/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.avro

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureConverterTest extends Specification with AvroUtils {

  "Avro2SimpleFeature should" should {

    val conf = ConfigFactory.parseString(
      """
        | converter = {
        |   type   = "avro"
        |   schema = "/schema.avsc"
        |   sft    = "testsft"
        |   id-field = "uuid()"
        |   fields = [
        |     { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
        |     { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
        |     { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
        |     { name = "geom", transform = "point($lon, $lat)" }
        |   ]
        | }
      """.stripMargin)

    "properly convert a GenericRecord to a SimpleFeature" >> {
      val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
      val converter = SimpleFeatureConverters.build[Array[Byte]](sft, conf)
      val sf = converter.processInput(Iterator.apply[Array[Byte]](bytes)).next()
      sf.getAttributeCount must be equalTo 1
    }
  }
}
