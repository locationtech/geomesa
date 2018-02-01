/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.io.ByteArrayInputStream

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureConverterTest extends Specification with AvroUtils {

  sequential

  "Avro2SimpleFeature should" should {

    val conf = ConfigFactory.parseString(
      """
        | {
        |   type        = "avro"
        |   schema-file = "/schema.avsc"
        |   sft         = "testsft"
        |   id-field    = "uuid()"
        |   fields = [
        |     { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
        |     { name = "dtg",  transform = "date('yyyy-MM-dd', avroPath($tobj, '/kvmap[$k=dtg]/v'))" },
        |     { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
        |     { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
        |     { name = "geom", transform = "point($lon, $lat)" }
        |   ]
        | }
      """.stripMargin)

    val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))

    "properly convert a GenericRecord to a SimpleFeature" >> {
      val converter = SimpleFeatureConverters.build[Array[Byte]](sft, conf)
      val ec = converter.createEvaluationContext()
      val sf = converter.processInput(Iterator.apply[Array[Byte]](bytes), ec).next()
      sf.getAttributeCount must be equalTo 2
      sf.getAttribute("dtg") must not beNull

      ec.counter.getFailure mustEqual 0L
      ec.counter.getSuccess mustEqual 1L
      ec.counter.getLineCount mustEqual 1L  // only 1 record passed in itr
    }

    "properly convert an input stream" >> {
      val converter = SimpleFeatureConverters.build[Array[Byte]](sft, conf)
      val ec = converter.createEvaluationContext()
      val sf = converter.process(new ByteArrayInputStream(bytes), ec).next()
      sf.getAttributeCount must be equalTo 2
      sf.getAttribute("dtg") must not beNull

      ec.counter.getFailure mustEqual 0L
      ec.counter.getSuccess mustEqual 1L
      ec.counter.getLineCount mustEqual 1L  // zero indexed so this is 2 records
    }

    "convert user data" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   schema-file = "/schema.avsc"
          |   sft         = "testsft"
          |   id-field    = "uuid()"
          |   user-data   = {
          |     my.user.key = "$lat"
          |   }
          |   fields = [
          |     { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
          |     { name = "dtg",  transform = "date('yyyy-MM-dd', avroPath($tobj, '/kvmap[$k=dtg]/v'))" },
          |     { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
          |     { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val converter = SimpleFeatureConverters.build[Array[Byte]](sft, conf)
      val ec = converter.createEvaluationContext()
      val sf = converter.processInput(Iterator.apply[Array[Byte]](bytes), ec).next()
      sf.getAttributeCount must be equalTo 2
      sf.getAttribute("dtg") must not(beNull)
      sf.getUserData.get("my.user.key") mustEqual 45d

      ec.counter.getFailure mustEqual 0L
      ec.counter.getSuccess mustEqual 1L
      ec.counter.getLineCount mustEqual 1L  // only 1 record passed in itr
    }
  }
}
