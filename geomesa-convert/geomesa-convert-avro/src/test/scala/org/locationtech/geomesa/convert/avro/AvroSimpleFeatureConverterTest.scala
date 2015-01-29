/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
      val sf = converter.processSingleInput(bytes)
      sf.getAttributeCount must be equalTo 1
    }
  }
}
