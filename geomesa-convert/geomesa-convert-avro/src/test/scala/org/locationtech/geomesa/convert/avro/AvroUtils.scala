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

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait AvroUtils {
  val spec = getClass.getResourceAsStream("/schema.avsc")

  val parser = new Parser
  val schema = parser.parse(spec)

  val contentSchema = schema.getField("content").schema()
  val types = contentSchema.getTypes.toList
  val tObjSchema = types(0)
  val otherObjSchema = types(1)

  val innerBuilder = new GenericRecordBuilder(tObjSchema.getField("kvmap").schema.getElementType)
  val rec1 = innerBuilder.set("k", "lat").set("v", 45.0).build
  val rec2 = innerBuilder.set("k", "lon").set("v", 45.0).build
  val rec3 = innerBuilder.set("k", "prop3").set("v", " foo ").build
  val rec4 = innerBuilder.set("k", "prop4").set("v", 1.0).build

  val outerBuilder = new GenericRecordBuilder(tObjSchema)
  val tObj = outerBuilder.set("kvmap", List(rec1, rec2, rec3, rec4).asJava).build()

  val compositeBuilder = new GenericRecordBuilder(schema)
  val obj = compositeBuilder.set("content", tObj).build()

  val otherObjBuilder = new GenericRecordBuilder(otherObjSchema)
  val otherObj = otherObjBuilder.set("id", 42).build()
  val obj2 = compositeBuilder.set("content", otherObj).build()

  val baos = new ByteArrayOutputStream()
  val writer = new GenericDatumWriter[GenericRecord](schema)
  val enc = EncoderFactory.get().binaryEncoder(baos, null)
  writer.write(obj, enc)
  enc.flush()
  baos.close()
  val bytes = baos.toByteArray
  val decoded = new GenericDatumReader[GenericRecord](schema).read(null, DecoderFactory.get().binaryDecoder(bytes, null))

  val datumReader = new GenericDatumReader[GenericRecord](schema)

  val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
  val gr1 = datumReader.read(null, decoder)

  val baos2 = new ByteArrayOutputStream()
  var enc2 = EncoderFactory.get().binaryEncoder(baos2, null)
  writer.write(obj2, enc2)
  enc2.flush()
  baos2.close()
  val bytes2 = baos2.toByteArray

  val decoder2 = DecoderFactory.get().binaryDecoder(bytes2, null)
  val gr2 = datumReader.read(null, decoder2)

}
