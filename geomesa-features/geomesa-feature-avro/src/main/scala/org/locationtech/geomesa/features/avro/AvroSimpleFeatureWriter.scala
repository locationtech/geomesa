/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.nio.ByteBuffer

import org.locationtech.jts.io.WKBWriter
import org.apache.avro.Schema
import org.apache.avro.io.{DatumWriter, Encoder}
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.serialization.AvroUserDataSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AvroSimpleFeatureWriter(sft: SimpleFeatureType, opts: Set[SerializationOption] = Set.empty)
  extends DatumWriter[SimpleFeature] {

  import AvroSimpleFeatureUtils._

  private val converters = {
    val nameEncoder = new FieldNameEncoder(VERSION)
    val typeMap = createTypeMap(sft, new WKBWriter(), nameEncoder)
    DataUtilities.attributeNames(sft).map(n => typeMap(nameEncoder.encode(n)).conv)
  }
  private val includeFid = !opts.withoutId
  private val includeUserData = opts.withUserData
  private val fieldOffset = if (includeFid) { 2 } else { 1 } // version + fid (optional)

  private var schema: Schema = generateSchema(sft, includeUserData, includeFid, sft.getName.getNamespaceURI)

  override def setSchema(s: Schema): Unit = schema = s

  override def write(datum: SimpleFeature, out: Encoder): Unit = {
    // write out the version and feature id
    out.writeInt(VERSION)
    if (includeFid) {
      out.writeString(datum.getID)
    }

    // write out fields from simple feature
    var i = 0
    while (i < converters.length) {
      val value = datum.getAttribute(i)
      val field = schema.getFields.get(i + fieldOffset)
      write(out, field.schema, if (value == null) { null } else { converters(i).apply(value) })
      i += 1
    }

    // write out user data
    if (includeUserData) {
      AvroUserDataSerialization.serialize(out, datum.getUserData)
    }
  }

  private def write(out: Encoder, fieldSchema: Schema, value: Any): Unit = {
    fieldSchema.getType match {
      case Schema.Type.STRING  => out.writeString(value.asInstanceOf[CharSequence])
      case Schema.Type.BYTES   => out.writeBytes(value.asInstanceOf[ByteBuffer])
      case Schema.Type.INT     => out.writeInt(value.asInstanceOf[Int])
      case Schema.Type.LONG    => out.writeLong(value.asInstanceOf[Long])
      case Schema.Type.DOUBLE  => out.writeDouble(value.asInstanceOf[Double])
      case Schema.Type.FLOAT   => out.writeFloat(value.asInstanceOf[Float])
      case Schema.Type.BOOLEAN => out.writeBoolean(value.asInstanceOf[Boolean])
      case Schema.Type.NULL    => out.writeNull()
      case Schema.Type.UNION   =>
        val unionIdx = if (value == null) { 1 } else { 0 }
        out.writeIndex(unionIdx)
        write(out, fieldSchema.getTypes.get(unionIdx), value)

      case t => throw new NotImplementedError(s"Unsupported Avro attribute type: $t")
    }
  }
}
