/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.data.Record
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.AbstractSimpleFeature.AbstractMutableSimpleFeature
import org.locationtech.geomesa.fs.storage.core.iceberg.RecordSimpleFeature.RecordConverter
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding.{GeoParquetNative, GeoParquetWkb}
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.Geometry

import java.nio.ByteBuffer
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.Date

/**
 * A simple feature implementation that wraps an iceberg record
 *
 * @param sft simple feature type
 * @param fields converters for the raw record fields
 * @param record the underlying record
 */
class RecordSimpleFeature(sft: SimpleFeatureType, fields: Array[RecordConverter], record: Record)
    extends AbstractMutableSimpleFeature(sft) {

  this.id = record.get(0, classOf[String])

  private val values = Array.ofDim[AnyRef](fields.length)

  override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = values(index) = value

  override def getAttribute(index: Int): AnyRef = {
    var cached = values(index)
    if (cached == null) {
      cached = fields(index).apply(record)
      values(index) = cached
    }
    cached
  }

  override lazy val getUserData: java.util.Map[AnyRef, AnyRef] = {
    val visibility = record.get(1, classOf[String])
    if (visibility == null) {
      java.util.Map.of()
    } else {
      java.util.Map.of(SecurityUtils.FEATURE_VISIBILITY, visibility)
    }
  }
}

object RecordSimpleFeature {

  def apply(schema: SimpleFeatureIcebergSchema): RecordFeatureFactory = {
    var i = 0
    var offset = 2 // 0 is fid, 1 is vis
    val converters = Array.ofDim[RecordConverter](schema.sft.getAttributeCount)
    while (i < converters.length) {
      val descriptor = schema.sft.getDescriptor(i)
      val types = ObjectType.selectType(descriptor)
      val (from, to) = Converter(types, schema)
      converters(i) = new RecordConverter(offset, from, to)
      if (types.head == ObjectType.GEOMETRY && offset + 1 < schema.schema.columns().size() &&
          schema.schema.columns().get(offset + 1).name().startsWith(SimpleFeatureSchema.InternalFieldDelimiter)) {
        offset += 1
      }
      offset += 1
      i += 1
    }

    new RecordFeatureFactory(schema.sft, converters)
  }

  class RecordFeatureFactory(sft: SimpleFeatureType, converters: Array[RecordConverter])
      extends (Record => SimpleFeature) {
    override def apply(record: Record): SimpleFeature = new RecordSimpleFeature(sft, converters, record)
  }

  private class RecordConverter(i: Int, fromFeature: Converter, toFeature: Converter) {
    def apply(record: Record): AnyRef = {
      val value = record.get(i)
      if (value == null) { null } else { toFeature(value) }
    }
    def apply(record: Record, value: AnyRef): Unit = {
      if (value == null) {
        record.set(i, null)
      } else {
        record.set(i, fromFeature(value))
      }
    }
  }

  private sealed trait Converter extends (AnyRef => AnyRef)

  private object Converter {
    def apply(types: Seq[ObjectType], schema: SimpleFeatureIcebergSchema): (Converter, Converter) = types.head match {
      case ObjectType.GEOMETRY if schema.geometries == GeoParquetWkb => (FromWkbConverter, ToWkbConverter)
      case ObjectType.GEOMETRY if schema.geometries == GeoParquetNative => throw new UnsupportedOperationException()
      case ObjectType.GEOMETRY => throw new UnsupportedOperationException("An implementation is missing")
      case ObjectType.DATE     => (FromDateConverter, ToDateConverter)
      case ObjectType.BYTES    => (FromBytesConverter, ToBytesConverter)
      case ObjectType.LIST     => ListConverter(types.last, schema)
      case ObjectType.MAP      => MapConverter(types(1), types(2), schema)
      case _ => (DirectConverter, DirectConverter)
    }
  }

  private object DirectConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = value
  }

  private object ToDateConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = Date.from(value.asInstanceOf[OffsetDateTime].toInstant)
  }
  private object FromDateConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = OffsetDateTime.ofInstant(value.asInstanceOf[Date].toInstant, ZoneOffset.UTC)
  }

  private object ToBytesConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val buffer = value.asInstanceOf[ByteBuffer]
      val pos = buffer.position()
      val buf = Array.ofDim[Byte](buffer.remaining())
      buffer.get(buf, 0, buf.length)
      buffer.position(pos)
      buf
    }
  }
  private object FromBytesConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
  }

  private object ListConverter {
    def apply(subtype: ObjectType, schema: SimpleFeatureIcebergSchema): (Converter, Converter) = {
      val (from, to) = Converter(Seq(subtype), schema)
      (new ListConverter(from), new ListConverter(to))
    }
  }

  private class ListConverter(subtype: Converter) extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val list = value.asInstanceOf[java.util.List[AnyRef]]
      val result = new java.util.ArrayList[AnyRef](list.size())
      list.forEach(v => result.add(subtype(v)))
      result
    }
  }

  private object MapConverter {
    def apply(keyType: ObjectType, valueType: ObjectType, schema: SimpleFeatureIcebergSchema): (Converter, Converter) = {
      val (keyFrom, keyTo) = Converter(Seq(keyType), schema)
      val (valueFrom, valueTo) = Converter(Seq(valueType), schema)
      (new MapConverter(keyFrom, valueFrom), new MapConverter(keyTo, valueTo))
    }
  }

  private class MapConverter(keyType: Converter, valueType: Converter) extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      val result = new java.util.HashMap[AnyRef, AnyRef](map.size())
      map.forEach { case (k, v) => result.put(keyType(k), valueType(v)) }
      result
    }
  }

  private object ToWkbConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val buffer = value.asInstanceOf[ByteBuffer]
      val pos = buffer.position()
      val buf = Array.ofDim[Byte](buffer.remaining())
      buffer.get(buf, 0, buf.length)
      buffer.position(pos)
      WKBUtils.read(buf)
    }
  }
  private object FromWkbConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = ByteBuffer.wrap(WKBUtils.write(value.asInstanceOf[Geometry]))
  }
}
