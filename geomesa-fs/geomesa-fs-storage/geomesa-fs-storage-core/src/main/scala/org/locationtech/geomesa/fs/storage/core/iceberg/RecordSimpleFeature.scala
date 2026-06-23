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
import org.locationtech.geomesa.fs.storage.core.parquet.schema.{ColumnName, SimpleFeatureParquetSchema}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.Geometry

import java.nio.ByteBuffer
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.Date

class RecordSimpleFeature(sft: SimpleFeatureType, fields: Array[RecordConverter], vis: Int, record: Record)
    extends AbstractMutableSimpleFeature(sft) {

  this.id = record.get(0, classOf[String])

  override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = fields(index).apply(record, value)

  override def getAttribute(index: Int): AnyRef = fields(index).apply(record)

  override def getUserData: java.util.Map[AnyRef, AnyRef] = {
    if (vis == -1) { java.util.Map.of() } else {
      val userData = new java.util.HashMap[AnyRef, AnyRef](1)
      userData.put(SecurityUtils.FEATURE_VISIBILITY, record.get(vis, classOf[String]))
      userData
    }
  }
}

object RecordSimpleFeature {

  def apply(schema: SimpleFeatureParquetSchema): RecordFeatureFactory = {
    var i = 0
    var vis = 1
    var offset = 2 // 0 is fid, 1 is vis
    if (!schema.hasVisibilities) {
      vis = -1
      offset = 1
    }
    val converters = Array.ofDim[RecordConverter](schema.sft.getAttributeCount)
    while (i < converters.length) {
      val descriptor = schema.sft.getDescriptor(i)
      descriptor.getType.getBinding match {
        case b if classOf[Geometry].isAssignableFrom(b) =>
          schema.geometries match {
            case GeoParquetWkb => converters(i) = new GeometryWkbConvert(offset)
            case GeoParquetNative => ??? // TODO
            case _ => throw new UnsupportedOperationException("An implementation is missing")
          }
          val col = ColumnName(descriptor.getLocalName)
          if (schema.bboxes.fields.exists(_.geometry == col)) {
            offset += 1
          }

        case b if classOf[Date].isAssignableFrom(b) => converters(i) = new DateConvert(offset)
        case _ => converters(i) = new DirectConverter(offset)
      }
      offset += 1
      i += 1
    }

    new RecordFeatureFactory(schema.sft, converters, vis)
  }

  class RecordFeatureFactory(sft: SimpleFeatureType, converters: Array[RecordConverter], vis: Int)
      extends (Record => SimpleFeature) {
    override def apply(record: Record): SimpleFeature = new RecordSimpleFeature(sft, converters, vis, record)
  }

  private sealed trait RecordConverter extends (Record => AnyRef) with ((Record, AnyRef) => Unit)

  private class DirectConverter(i: Int) extends RecordConverter {
    override def apply(record: Record): AnyRef = record.get(i)
    override def apply(record: Record, value: AnyRef): Unit = record.set(i, value)
  }

  private class DateConvert(i: Int) extends RecordConverter {
    override def apply(record: Record): AnyRef = Date.from(record.get(i).asInstanceOf[OffsetDateTime].toInstant)
    override def apply(record: Record, value: AnyRef): Unit =
      record.set(i, OffsetDateTime.ofInstant(value.asInstanceOf[Date].toInstant, ZoneOffset.UTC))
  }

  private class GeometryWkbConvert(i: Int) extends RecordConverter {
    private var buf: Array[Byte] = Array.ofDim(1024)
    override def apply(record: Record): AnyRef = {
      val buffer = record.get(i, classOf[ByteBuffer])
      if (buf.length < buffer.remaining()) {
        buf = Array.ofDim((buffer.remaining() * 1.2).toInt)
      }
      buffer.get(buf, 0, buffer.remaining())
      WKBUtils.read(buf)
    }
    override def apply(record: Record, value: AnyRef): Unit = {
      // TODO is it ok to just mutate the buffer? it may make a defensive copy
      val buffer = record.get(i, classOf[ByteBuffer])
      buffer.reset()
      buffer.put(WKBUtils.write(value.asInstanceOf[Geometry]))
      buffer.rewind()
    }
  }
}
