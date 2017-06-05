/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.nio.ByteBuffer

import com.vividsolutions.jts.io.WKBWriter
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.io.{DatumWriter, Encoder}
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.serialization.AvroUserDataSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AvroSimpleFeatureWriter(sft: SimpleFeatureType, opts: Set[SerializationOption] = Set.empty)
  extends DatumWriter[SimpleFeature] {

  import AvroSimpleFeatureUtils._

  private var schema: Schema =
    generateSchema(sft,
      withUserData = opts.withUserData,
      namespace = sft.getName.getNamespaceURI)

  private val nameEncoder = new FieldNameEncoder(VERSION)
  private val typeMap = createTypeMap(sft, new WKBWriter(), nameEncoder)
  private val names = DataUtilities.attributeNames(sft).map(nameEncoder.encode)
  private var lastDataIdx = getLastDataIdx

  private def getLastDataIdx = schema.getFields.size() - (if (opts.withUserData) 1 else 0)

  override def setSchema(s: Schema): Unit = {
    schema = s
    lastDataIdx = getLastDataIdx
  }

  def defaultWrite(datum: SimpleFeature, out: Encoder) = {

    def rawField(field: Field) = datum.getAttribute(field.pos - 2)

    def getFieldValue[T](field: Field): T =
      if (rawField(field) == null)
        null.asInstanceOf[T]
      else
        convertValue(field.pos - 2, rawField(field)).asInstanceOf[T]

    def write(schema: Schema, f: Field): Unit = {
      import Schema.Type._

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
    while (i < lastDataIdx) {
      val f = schema.getFields.get(i)
      write(f.schema, f)
      i += 1
    }
  }

  def writeWithUserData(datum: SimpleFeature, out: Encoder) = {
    defaultWrite(datum, out)
    AvroUserDataSerialization.serialize(out, datum.getUserData)
  }

  private val writer: (SimpleFeature, Encoder) => Unit = if (opts.withUserData) writeWithUserData else defaultWrite

  override def write(datum: SimpleFeature, out: Encoder): Unit = writer(datum, out)

  private def convertValue(idx: Int, v: AnyRef) = typeMap(names(idx)).conv.apply(v)
}
