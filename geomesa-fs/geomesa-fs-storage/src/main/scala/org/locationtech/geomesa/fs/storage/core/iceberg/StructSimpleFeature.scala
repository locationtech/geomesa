/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.{Accessor, MetadataColumns, StructLike}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.identity.FeatureId
import org.locationtech.geomesa.features.AbstractSimpleFeature.AbstractMutableSimpleFeature
import org.locationtech.geomesa.fs.storage.core.iceberg.StructSimpleFeature.ColumnAccessor
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding.GeoParquetWkb
import org.locationtech.geomesa.fs.storage.core.schema.{ColumnName, SimpleFeatureSchema}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.WKBUtils

import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

/**
 * A simple feature implementation that wraps an iceberg record
 *
 * @param sft simple feature type
 * @param fields converters for the raw record fields
 */
class StructSimpleFeature(
    sft: SimpleFeatureType,
    hasIds: Boolean,
    fields: Array[ColumnAccessor],
    filePathAccessor: Accessor[StructLike],
    rowPosAccessor: Accessor[StructLike],
  ) extends AbstractMutableSimpleFeature(sft) {

  private var row: StructLike = _
  private var userData: java.util.Map[AnyRef, AnyRef] = _

  private val values = Array.ofDim[AnyRef](fields.length)
  private val visCol = if (hasIds) { 1 } else { 0 }

  def setRow(row: StructLike): StructSimpleFeature = {
    this.row = row
    this.id = null
    this.userData = null
    var i = 0
    while (i < values.length) {
      values(i) = null
      i += 1
    }
    this
  }

  /**
   * Gets the full file path that this feature came from. Note - this requires the path to be selected on read, otherwise
   * the behavior of this method is not defined
   *
   * @return
   */
  def getFilePath: String = filePathAccessor.get(row).asInstanceOf[String]

  /**
   * Gets the row number in the file that this feature came from. Note - this requires the row to be selected on read, otherwise
   * the behavior of this method is not defined
   *
   * @return
   */
  def getRowPosition: Long = rowPosAccessor.get(row).asInstanceOf[java.lang.Long]

  override def getIdentifier: FeatureId = { getID; super.getIdentifier }

  override def getID: String = {
    if (id == null) {
      if (hasIds) {
        id = row.get(0, classOf[String])
      } else {
        id = java.lang.Long.toString(StructSimpleFeature.IdCounter.getAndIncrement())
      }
    }
    id
  }

  override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = values(index) = value

  override def getAttribute(index: Int): AnyRef = {
    var cached = values(index)
    if (cached == null) {
      cached = fields(index).apply(row)
      values(index) = cached
    }
    cached
  }

  override def getUserData: java.util.Map[AnyRef, AnyRef] = {
    if (userData == null) {
      userData = new java.util.HashMap(1)
      val visibility = row.get(visCol, classOf[String])
      if (visibility != null) {
        userData.put(SecurityUtils.FEATURE_VISIBILITY, visibility)
      }
    }
    userData
  }
}

object StructSimpleFeature {

  import scala.collection.JavaConverters._

  private val IdCounter = new AtomicLong(0)

  def apply(schema: SimpleFeatureIcebergSchema): StructSimpleFeature = {
    var i = 0
    val accessors = Array.ofDim[ColumnAccessor](schema.sft.getAttributeCount)
    val cols = schema.schema.columns().asScala
    val hasId = cols.headOption.exists(_.name() == SimpleFeatureSchema.FeatureIdField)
    while (i < accessors.length) {
      val descriptor = schema.sft.getDescriptor(i)
      val converter = Converter(descriptor)
      val col = ColumnName.encode(descriptor.getLocalName)
      val offset = cols.indexWhere(_.name() == col)
      accessors(i) = converter match {
        case None => new DirectAccessor(offset)
        case Some(c) => new ConverterAccessor(offset, c)
      }
      i += 1
    }
    val filePathAccessor = schema.schema.accessorForField(MetadataColumns.FILE_PATH.fieldId())
    val rowPosAccessor = schema.schema.accessorForField(MetadataColumns.ROW_POSITION.fieldId())

    new StructSimpleFeature(schema.sft, hasId, accessors, filePathAccessor, rowPosAccessor)
  }

  private sealed trait ColumnAccessor extends (StructLike => AnyRef) {
    def apply(row: StructLike): AnyRef
  }

  private class DirectAccessor(i: Int) extends ColumnAccessor {
    override def apply(row: StructLike): AnyRef = row.get(i, classOf[AnyRef])
  }

  private class ConverterAccessor(i: Int, converter: Converter) extends ColumnAccessor {
    override def apply(row: StructLike): AnyRef = {
      val value = row.get(i, classOf[AnyRef])
      if (value == null) { null } else { converter(value) }
    }
  }

  private sealed trait Converter extends (AnyRef => AnyRef)

  private object Converter {
    def apply(descriptor: AttributeDescriptor): Option[Converter] = {
      val types = ObjectType.selectType(descriptor)
      if (types.head == ObjectType.GEOMETRY) {
        val encoding = descriptor.getUserData.get(SimpleFeatureIcebergSchema.GeometryEncodingKey) match {
          case e: String => GeometryEncoding(e)
          case _ => GeometryEncoding.GeoParquetWkb
        }
        encoding match {
          case GeoParquetWkb => Some(FromWkbConverter)
          case _ => throw new UnsupportedOperationException(encoding.toString)
        }
      } else if (types.head == ObjectType.LIST) {
        primitive(types.last).map(new ListConverter(_))
      } else if (types.head == ObjectType.MAP) {
        val keyConverter = primitive(types(1))
        val valueConverter = primitive(types(2))
        (keyConverter, valueConverter) match {
          case (None, None) => None
          case (Some(k), None) => Some(new MapKeyConverter(k))
          case (None, Some(v)) => Some(new MapValueConverter(v))
          case (Some(k), Some(v)) => Some(new MapConverter(k, v))
        }
      } else {
        primitive(types.head)
      }
    }

    private def primitive(types: ObjectType): Option[Converter] = types match {
      case ObjectType.DATE  => Some(FromDateConverter)
      case ObjectType.BYTES => Some(FromBytesConverter)
      case _ => None
    }
  }

  private object FromDateConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = Date.from(value.asInstanceOf[OffsetDateTime].toInstant)
  }

  private object FromBytesConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val buffer = value.asInstanceOf[ByteBuffer]
      val pos = buffer.position()
      val buf = Array.ofDim[Byte](buffer.remaining())
      buffer.get(buf, 0, buf.length)
      buffer.position(pos)
      buf
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

  private class MapConverter(keyType: Converter, valueType: Converter) extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      val result = new java.util.HashMap[AnyRef, AnyRef](map.size())
      map.forEach { case (k, v) => result.put(keyType(k), valueType(v)) }
      result
    }
  }

  private class MapKeyConverter(keyType: Converter) extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      val result = new java.util.HashMap[AnyRef, AnyRef](map.size())
      map.forEach { case (k, v) => result.put(keyType(k), v) }
      result
    }
  }

  private class MapValueConverter(valueType: Converter) extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      val result = new java.util.HashMap[AnyRef, AnyRef](map.size())
      map.forEach { case (k, v) => result.put(k, valueType(v)) }
      result
    }
  }

  private object FromWkbConverter extends Converter {
    override def apply(value: AnyRef): AnyRef = {
      val buffer = value.asInstanceOf[ByteBuffer]
      val pos = buffer.position()
      val buf = Array.ofDim[Byte](buffer.remaining())
      buffer.get(buf, 0, buf.length)
      buffer.position(pos)
      WKBUtils.read(buf)
    }
  }
}
