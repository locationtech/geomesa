/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro.registry

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait AvroSchemaRegistryUtils {
  val spec = getClass.getResourceAsStream("/schema.avsc")
  val spec2 = getClass.getResourceAsStream("/schema2.avsc")

  val parser = new Parser
  val schema = parser.parse(spec)
  val schema2 = parser.parse(spec2)

  val contentSchema = schema.getField("content").schema()
  val types = contentSchema.getTypes.toList
  val tObjSchema = types(0)
  val otherObjSchema = types(1)

  val contentSchema2 = schema2.getField("content").schema()
  val types2 = contentSchema2.getTypes.toList
  val tObjSchema2 = types2(0)
  val otherObjSchema2 = types2(1)

  val innerBuilder = new GenericRecordBuilder(tObjSchema.getField("kvmap").schema.getElementType)
  val rec1 = innerBuilder.set("k", "lat").set("v", 45.0).build
  val rec2 = innerBuilder.set("k", "lon").set("v", 45.0).build
  val rec3 = innerBuilder.set("k", "prop3").set("v", " foo ").build
  val rec4 = innerBuilder.set("k", "prop4").set("v", 1.0).build
  val rec5 = innerBuilder.set("k", "dtg").set("v", "2015-01-02").build

  val outerBuilder = new GenericRecordBuilder(tObjSchema)
  val tObj = outerBuilder.set("kvmap", List(rec1, rec2, rec3, rec4, rec5).asJava).build()

  val compositeBuilder = new GenericRecordBuilder(schema)
  val obj = compositeBuilder.set("content", tObj).build()

  val innerBuilder2 = new GenericRecordBuilder(tObjSchema2.getField("kvmap").schema.getElementType)
  val rec2_1 = innerBuilder2.set("k", "lat").set("v", 45.0).build
  val rec2_2 = innerBuilder2.set("k", "lon").set("v", 45.0).build
  val rec2_3 = innerBuilder2.set("k", "prop3").set("v", " foo ").build
  val rec2_4 = innerBuilder2.set("k", "prop4").set("v", 1.0).build
  val rec2_5 = innerBuilder2.set("k", "dtg").set("v", "2015-01-02").build
  val rec2_6 = innerBuilder2.set("k", "extra").set("v", null).set("extra", "TEST").build

  val outerBuilder2 = new GenericRecordBuilder(tObjSchema2)
  val tObj2 = outerBuilder2.set("kvmap", List(rec2_1, rec2_2, rec2_3, rec2_4, rec2_5, rec2_6).asJava).build()

  val compositeBuilder2 = new GenericRecordBuilder(schema2)

  val obj2 = compositeBuilder2.set("content", tObj2).build()

  val baos = new ByteArrayOutputStream()
  val writer2_1 = new GenericDatumWriter[GenericRecord](schema)
  val writer2_2 = new GenericDatumWriter[GenericRecord](schema2)
  var enc3 = EncoderFactory.get().binaryEncoder(baos, null)
  val header1 = Array[Byte](0,0,0,0,1)
  val header2 = Array[Byte](0,0,0,0,2)

  baos.write(header1)
  writer2_1.write(obj, enc3)
  enc3.flush()

  baos.write(header2)
  writer2_2.write(obj2, enc3)
  enc3.flush()

  baos.write(header2)
  writer2_2.write(obj2, enc3)
  enc3.flush()

  baos.close()
  val bytes = baos.toByteArray
}
