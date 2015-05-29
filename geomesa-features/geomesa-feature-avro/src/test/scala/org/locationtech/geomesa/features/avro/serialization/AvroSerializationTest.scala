/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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
package org.locationtech.geomesa.features.avro.serialization

import org.apache.avro.io.{Decoder, Encoder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.serialization.{AbstractReader, AbstractWriter}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSerializationTest extends Specification {

  sequential

  "encodings cache" should {

    "create a reader" >> {
      val reader: AbstractReader[Decoder] = AvroSerialization.reader
      reader must not(beNull)
    }

    "create encodings" >> {
      val sft = SimpleFeatureTypes.createType("test type", "name:String,*geom:Point,dtg:Date")
      val encodings = AvroSerialization.decodings(sft).forVersion(version = 1)
      encodings must haveSize(3)
    }
  }

  "decodings cache" should {

    "create a writer" >> {
      val writer: AbstractWriter[Encoder] = AvroSerialization.writer
      writer must not(beNull)
    }

    "create encodings" >> {
      val sft = SimpleFeatureTypes.createType("test type", "name:String,*geom:Point,dtg:Date")
      val encodings = AvroSerialization.encodings(sft)
      encodings must haveSize(3)
    }
  }
}

