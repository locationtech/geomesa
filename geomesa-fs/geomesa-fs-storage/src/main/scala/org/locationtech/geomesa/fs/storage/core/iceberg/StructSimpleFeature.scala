/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.google.gson._
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.TimestampType
import org.apache.iceberg.{Accessor, MetadataColumns, StructLike}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.identity.FeatureId
import org.locationtech.geomesa.features.AbstractSimpleFeature.AbstractMutableSimpleFeature
import org.locationtech.geomesa.fs.storage.core.iceberg.StructSimpleFeature.ColumnAccessor
import org.locationtech.geomesa.fs.storage.core.parquet.io.StructuralJson
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding.GeoParquetWkb
import org.locationtech.geomesa.fs.storage.core.schema.{ColumnName, SimpleFeatureSchema}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.text.WKBUtils

import java.nio.ByteBuffer
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime}
import java.util.concurrent.atomic.AtomicLong
import java.util.{Date, UUID}

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
      val col = ColumnName.encode(descriptor.getLocalName)
      val offset = cols.indexWhere(_.name() == col)
      accessors(i) = Converter(descriptor, col, schema) match {
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
    def apply(descriptor: AttributeDescriptor, col: String, schema: SimpleFeatureIcebergSchema): Option[Converter] = {
      val types = ObjectType.selectType(descriptor)
      if (types.head == ObjectType.GEOMETRY) {
        val encoding = descriptor.getUserData.get(SimpleFeatureIcebergSchema.GeometryEncodingKey) match {
          case e: String => GeometryEncoding(e)
          case _ => GeometryEncoding.GeoParquetWkb
        }
        if (encoding != GeoParquetWkb) {
          throw new UnsupportedOperationException(encoding.toString)
        }
        Some(FromWkbConverter)
      } else if (types.last == ObjectType.JSON && descriptor.getJsonSchema().isDefined) {
        Some(new StructJsonConverter(schema.schema.findType(col)))
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

  /**
   * Converter for a structural-JSON attribute. Walks the materialized iceberg value (struct/list/map/leaf)
   * against its iceberg type and rebuilds a compact JSON string, so the value round-trips as a JSON attribute.
   */
  private class StructJsonConverter(t: org.apache.iceberg.types.Type) extends Converter {
    override def apply(value: AnyRef): AnyRef = StructuralJson.compact(toJson(t, value))

    private def toJson(t: org.apache.iceberg.types.Type, value: AnyRef): JsonElement = {
      if (value == null) { JsonNull.INSTANCE } else {
        t.typeId() match {
          case TypeID.STRUCT =>
            val obj = new JsonObject()
            val fields = t.asStructType().fields()
            val row = value.asInstanceOf[StructLike]
            var i = 0
            while (i < fields.size()) {
              val field = fields.get(i)
              val fieldValue = row.get(i, classOf[AnyRef])
              if (fieldValue != null) {
                obj.add(field.name(), toJson(field.`type`(), fieldValue))
              }
              i += 1
            }
            obj

          case TypeID.LIST =>
            val array = new JsonArray()
            val elementType = t.asListType().elementType()
            value.asInstanceOf[java.util.List[AnyRef]].forEach(v => array.add(toJson(elementType, v)))
            array

          case TypeID.MAP =>
            val obj = new JsonObject()
            val valueType = t.asMapType().valueType()
            value.asInstanceOf[java.util.Map[AnyRef, AnyRef]].forEach { (k, v) =>
              obj.add(String.valueOf(k), toJson(valueType, v))
            }
            obj

          case TypeID.BOOLEAN => new JsonPrimitive(value.asInstanceOf[java.lang.Boolean])
          case TypeID.INTEGER => new JsonPrimitive(value.asInstanceOf[java.lang.Integer])
          case TypeID.LONG    => new JsonPrimitive(value.asInstanceOf[java.lang.Long])
          case TypeID.FLOAT   => new JsonPrimitive(value.asInstanceOf[java.lang.Float])
          case TypeID.DOUBLE  => new JsonPrimitive(value.asInstanceOf[java.lang.Double])
          case TypeID.STRING  => new JsonPrimitive(value.toString)
          case TypeID.DECIMAL => new JsonPrimitive(value.asInstanceOf[java.math.BigDecimal])
          case TypeID.UUID    => new JsonPrimitive(value.asInstanceOf[UUID].toString)
          case TypeID.DATE    => new JsonPrimitive(StructuralJson.dateToJson(value.asInstanceOf[LocalDate]))
          case TypeID.TIME    => new JsonPrimitive(StructuralJson.timeToJson(value.asInstanceOf[LocalTime]))

          case TypeID.TIMESTAMP =>
            if (t.asInstanceOf[TimestampType].shouldAdjustToUTC()) {
              new JsonPrimitive(StructuralJson.timestampToJson(value.asInstanceOf[OffsetDateTime]))
            } else {
              new JsonPrimitive(StructuralJson.timestampToJson(value.asInstanceOf[LocalDateTime]))
            }

          case TypeID.FIXED | TypeID.BINARY =>
            new JsonPrimitive(StructuralJson.bytesToJson(value.asInstanceOf[ByteBuffer]))

          case id =>
            throw new UnsupportedOperationException(s"No structural JSON mapping defined for iceberg type: $id")
        }
      }
    }
  }
}
