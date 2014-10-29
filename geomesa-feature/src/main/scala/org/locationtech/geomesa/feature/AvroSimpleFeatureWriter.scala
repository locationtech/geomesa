/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.feature

import java.nio.ByteBuffer

import com.vividsolutions.jts.io.WKBWriter
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type._
import org.apache.avro.io.{DatumWriter, Encoder}
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.feature.AvroSimpleFeatureUtils._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class AvroSimpleFeatureWriter(sft: SimpleFeatureType)
  extends DatumWriter[SimpleFeature] {

  private var schema: Schema = generateSchema(sft)
  private val typeMap = createTypeMap(sft, new WKBWriter())
  private val names = DataUtilities.attributeNames(sft).map(encodeAttributeName)

  override def setSchema(s: Schema): Unit = schema = s

  override def write(datum: SimpleFeature, out: Encoder): Unit = {

    def rawField(field: Field) = datum.getAttribute(field.pos - 2)

    def getFieldValue[T](field: Field): T =
      if (rawField(field) == null)
        null.asInstanceOf[T]
      else
        convertValue(field.pos - 2, rawField(field)).asInstanceOf[T]

    def write(schema: Schema, f: Field): Unit = {
      schema.getType match {
        case UNION   =>
          val unionIdx = if (rawField(f) == null) 1 else 0
          out.writeIndex(unionIdx)
          write(schema.getTypes.get(unionIdx), f)
        case STRING  => out.writeString(getFieldValue[CharSequence](f))
        case BYTES   => out.writeBytes(getFieldValue[ByteBuffer](f))
        case INT     => out.writeInt(getFieldValue[Int](f))
        case LONG    => out.writeLong(getFieldValue[Long](f))
        case DOUBLE  => out.writeDouble(getFieldValue[Double](f))
        case FLOAT   => out.writeFloat(getFieldValue[Float](f))
        case BOOLEAN => out.writeBoolean(getFieldValue[Boolean](f))
        case NULL    => out.writeNull()
        case _ => throw new RuntimeException("unsupported avro simple feature type")
      }
    }

    // write out the version and feature ID first
    out.writeInt(VERSION)
    out.writeString(datum.getID)

    // Write out fields from Simple Feature
    var i = 2
    while(i < schema.getFields.length) {
      val f = schema.getFields.get(i)
      write(f.schema, f)
      i += 1
    }
  }

  private def convertValue(idx: Int, v: AnyRef) = typeMap(names(idx)).conv.apply(v)
}
