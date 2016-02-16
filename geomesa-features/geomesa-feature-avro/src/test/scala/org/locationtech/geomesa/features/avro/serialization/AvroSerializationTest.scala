/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.SchemaBuilder
import org.apache.avro.io.{Decoder, DecoderFactory, Encoder, EncoderFactory}
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

  "avro encoding" should {
    "properly call end item on arrays" >> {

      // For reference this is a schema that fails without overridden readGenericMap method in AvroReader
      val schema = SchemaBuilder.record("myrecord")
        .namespace("foobar.testing")
        .fields
        .name("field1").`type`.intType.noDefault
        .name("field2").`type`.stringType.noDefault
        .name("field3").`type`.array().items().record("arr").fields()
          .name("class").`type`.stringType().noDefault()
          .name("key").`type`.stringType().noDefault()
          .name("value").`type`.stringType().noDefault().endRecord().noDefault()
        .name("field4").`type`.stringType.noDefault

      val writer = new AvroWriter
      val baos = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
      writer.writeInt.apply(encoder, 123)
      writer.writeString.apply(encoder, "str1")
      import scala.collection.JavaConverters._
      writer.writeGenericMap.apply(encoder, Map[AnyRef, AnyRef]("k1" -> new java.lang.Integer(123), "k2" -> "23623623").asJava)
      writer.writeString.apply(encoder, "my string")

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val decoder = DecoderFactory.get().directBinaryDecoder(bais, null)
      val reader = new AvroReader
      reader.readInt.apply(decoder) mustEqual 123
      reader.readString.apply(decoder) mustEqual "str1"
      val m = reader.readGenericMap(2).apply(decoder)
      import scala.collection.JavaConversions._
      m.keySet().toSeq must containTheSameElementsAs(Seq("k1", "k2"))

      // this last test fails without the overridden readGenericMap in AvroReader
      // due to data still in the pipe or some pointer advancing or something.
      reader.readString.apply(decoder) mustEqual "my string"
    }
  }
}

