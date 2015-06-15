/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
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

