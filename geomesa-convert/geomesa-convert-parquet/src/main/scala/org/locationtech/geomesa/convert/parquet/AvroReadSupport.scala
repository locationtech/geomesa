/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.conf.ParquetConfiguration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.locationtech.geomesa.convert.parquet.AvroReadSupport.AvroRecordMaterializer
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureReadSupport._
import org.locationtech.geomesa.fs.storage.parquet.io.{SimpleFeatureParquetSchema, SimpleFeatureReadSupport}
import org.locationtech.geomesa.utils.geotools.ObjectType

import java.util.{Collections, Date}

/**
  * Read support for parsing an arbitrary parquet file into avro records.
  *
  * The official parquet-avro reader doesn't support 'repeated' columns, and requires avro 1.8,
  * so we roll our own
  */
class AvroReadSupport extends ReadSupport[GenericRecord] {

  import scala.collection.JavaConverters._

  private var schema: Option[SimpleFeatureParquetSchema] = None

  override def init(context: InitContext): ReadContext = {
    schema = SimpleFeatureParquetSchema.read(context)
    new ReadContext(context.getFileSchema, schema.map(_.metadata).getOrElse(Collections.emptyMap()))
  }

  override def prepareForRead(
      configuration: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[GenericRecord] =
    new AvroRecordMaterializer(fileSchema.getFields.asScala.toSeq, schema)

  override def prepareForRead(
    configuration: ParquetConfiguration,
    keyValueMetaData: java.util.Map[String, String],
    fileSchema: MessageType,
    readContext: ReadContext): RecordMaterializer[GenericRecord] =
    new AvroRecordMaterializer(fileSchema.getFields.asScala.toSeq, schema)
}

object AvroReadSupport {

  import scala.collection.JavaConverters._

  class AvroRecordMaterializer(fields: Seq[Type], schema: Option[SimpleFeatureParquetSchema])
      extends RecordMaterializer[GenericRecord] {
    private val root = new GenericGroupConverter(fields, schema)
    override def getCurrentRecord: GenericRecord = root.materialize()
    override def getRootConverter: GroupConverter = root
  }

  /**
    * Schema-less implementation of GenericRecord
    *
    * @param fields field names
    */
  private class AvroRecord(fields: IndexedSeq[String]) extends GenericRecord {

    private val values = Array.ofDim[AnyRef](fields.length)

    override def put(key: String, v: AnyRef): Unit = values(fields.indexOf(key)) = v
    override def get(key: String): AnyRef = values(fields.indexOf(key))
    override def put(i: Int, v: AnyRef): Unit = values(i) = v
    override def get(i: Int): AnyRef = values(i)

    override def getSchema: Schema = null

    override def toString: String = {
      val builder = new StringBuilder("AvroRecord[")
      var i = 0
      while (i < fields.length) {
        if (i != 0) {
          builder.append(',')
        }
        builder.append(fields(i)).append(':').append(values(i))
        i += 1
      }
      builder.append(']')
      builder.toString
    }
  }

  /**
   * Group converter for a record
   *
   * @param fields fields
   * @param schema geomesa schema encodings, if it's a geomesa file
   */
  private class GenericGroupConverter(fields: Seq[Type], schema: Option[SimpleFeatureParquetSchema])
      extends GroupConverter with ValueMaterializer[GenericRecord] {

    private val names = fields.map(_.getName).toIndexedSeq
    private val converters = schema match {
      case None => Array.tabulate(fields.length)(i => converter(fields(i)))
      case Some(s) =>
        // for attributes, we re-use our parquet read support so that they get parsed into standard simple feature attribute
        // types (including geometries) instead of generic avro types
        val attributes =
          s.sft.getAttributeDescriptors.asScala.map(d => SimpleFeatureReadSupport.attribute(ObjectType.selectType(d), s.encodings))
        // the remaining non-attribute fields are added as generic types (currently fid, vis, and bboxes)
        val remaining = fields.drop(s.sft.getAttributeCount).map(converter).toArray
        attributes.toArray ++ remaining
    }

    private var rec: AvroRecord = _

    override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)
    override def start(): Unit = {
      rec = new AvroRecord(names)
      converters.foreach(_.reset())
    }
    override def end(): Unit = {
      var i = 0
      while (i < fields.length) {
        rec.put(i, converters(i).materialize())
        i += 1
      }
    }
    override def reset(): Unit = rec = null
    override def materialize(): GenericRecord = rec
  }

  /**
    * Get a converter for a field type
    *
    * @param field field
    * @return
    */
  private def converter(field: Type): ValueMaterializer[_ <: AnyRef] = {
    val logical = field.getLogicalTypeAnnotation
    if (field.isPrimitive) {
      lazy val logicalConverter = logical match {
        case _: StringLogicalTypeAnnotation => Some(new StringConverter())
        case _: DateLogicalTypeAnnotation => Some(new DaysConverter())
        case t: TimestampLogicalTypeAnnotation if t.getUnit == LogicalTypeAnnotation.TimeUnit.MILLIS => Some(new DateMillisConverter())
        case t: TimestampLogicalTypeAnnotation if t.getUnit == LogicalTypeAnnotation.TimeUnit.MICROS => Some(new DateMicrosConverter())
        case t: TimestampLogicalTypeAnnotation if t.getUnit == LogicalTypeAnnotation.TimeUnit.NANOS => Some(new NanosConverter())
        case _: UUIDLogicalTypeAnnotation => Some(new UuidConverter())
        case _ => None
      }
      val convert = field.asPrimitiveType().getPrimitiveTypeName match {
        case PrimitiveTypeName.BINARY               => logicalConverter.getOrElse(new GeneralPrimitiveConverter())
        case PrimitiveTypeName.INT32                => logicalConverter.getOrElse(new GeneralPrimitiveConverter())
        case PrimitiveTypeName.INT64                => logicalConverter.getOrElse(new GeneralPrimitiveConverter())
        case PrimitiveTypeName.DOUBLE               => new GeneralPrimitiveConverter()
        case PrimitiveTypeName.FLOAT                => new GeneralPrimitiveConverter()
        case PrimitiveTypeName.BOOLEAN              => new GeneralPrimitiveConverter()
        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => logicalConverter.getOrElse(new GeneralPrimitiveConverter())
        case _                                      => NullPrimitiveConverter
      }
      if (field.isRepetition(Repetition.REPEATED)) {
        new RepeatedPrimitiveConverter(convert)
      } else {
        convert
      }
    } else {
      val group = field.asGroupType()
      logical match {
        case _: ListLogicalTypeAnnotation =>
          require(group.getFieldCount == 1 && !group.getType(0).isPrimitive, s"Invalid list type: $group")
          val list = group.getType(0).asGroupType()
          require(list.getFieldCount == 1 && list.isRepetition(Repetition.REPEATED), s"Invalid list type: $group")
          val elements = list.getType(0)
          require(!elements.isPrimitive, s"Invalid list type: $group")
          val elementGroup = elements.asGroupType()
          require(elementGroup.getFieldCount == 1 && elementGroup.getType(0).isPrimitive, s"Invalid list type: $group")
          new ListConverter(converter(list.getType(0)))

        case _: MapLogicalTypeAnnotation =>
          require(group.getFieldCount == 1 && !group.getType(0).isPrimitive, s"Invalid map type: $group")
          val map = group.getType(0).asGroupType()
          require(map.getFieldCount == 2 && map.isRepetition(Repetition.REPEATED), s"Invalid map type: $group")
          new MapConverter(converter(map.getType(0)), converter(map.getType(1)))

        case _ =>
          new GenericGroupConverter(group.getFields.asScala.toSeq, None)
      }
    }
  }

  /**
   * Converter for primitive fields with repetition of 'repeated'
   *
   * @param delegate single value converter
   */
  private class RepeatedPrimitiveConverter(delegate: PrimitiveConverter with ValueMaterializer[_ <: AnyRef])
    extends PrimitiveConverter with ValueMaterializer[java.util.List[AnyRef]] {

    private var list: java.util.List[AnyRef] = _

    override def addBinary(value: Binary): Unit = { delegate.addBinary(value); addElement() }
    override def addBoolean(value: Boolean): Unit = { delegate.addBoolean(value); addElement() }
    override def addInt(value: Int): Unit = { delegate.addInt(value); addElement() }
    override def addFloat(value: Float): Unit = { delegate.addFloat(value); addElement() }
    override def addLong(value: Long): Unit = { delegate.addLong(value); addElement() }
    override def addDouble(value: Double): Unit = { delegate.addDouble(value); addElement() }

    private def addElement(): Unit = {
      if (list == null) {
        list = new java.util.ArrayList[AnyRef]()
      }
      list.add(delegate.materialize())
      delegate.reset()
    }
    override def reset(): Unit = list = null
    override def materialize(): java.util.List[AnyRef] = list
  }

  /**
    * Converter for a DAYS encoded INT32 field
    */
  private class DaysConverter extends PrimitiveConverter with ValueMaterializer[Date] {
    private var value: Int = -1
    private var set = false

    override def addInt(value: Int): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): Date = if (set) { Date.from(BinnedTime.Epoch.plusDays(value).toInstant) } else { null }
  }

  /**
   * Converter for a MICROS encoded INT64 field
   */
  private class NanosConverter extends PrimitiveConverter with ValueMaterializer[Date] {
    private var value: Long = -1
    private var set = false

    override def addLong(value: Long): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): Date = if (set) { new Date(value / 1000000L) } else { null }
  }

  private case object NullPrimitiveConverter extends PrimitiveConverter with ValueMaterializer[AnyRef] {
    override def addBinary(value: Binary): Unit = {}
    override def addBoolean(value: Boolean): Unit = {}
    override def addDouble(value: Double): Unit = {}
    override def addFloat(value: Float): Unit = {}
    override def addInt(value: Int): Unit = {}
    override def addLong(value: Long): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = null
  }

  /**
   * Converter for any primitive type
   */
  private class GeneralPrimitiveConverter extends PrimitiveConverter with ValueMaterializer[AnyRef] {
    private var value: Any = -1
    private var set = false

    override def addBinary(value: Binary): Unit = {
      if (value != null) {
        this.value = value.getBytes
        set = true
      }
    }
    override def addBoolean(value: Boolean): Unit = {
      this.value = value
      set = true
    }
    override def addDouble(value: Double): Unit = {
      this.value = value
      set = true
    }
    override def addFloat(value: Float): Unit = {
      this.value = value
      set = true
    }
    override def addInt(value: Int): Unit = {
      this.value = value
      set = true
    }
    override def addLong(value: Long): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): AnyRef = if (set) { value.asInstanceOf[AnyRef] } else { null }
  }
}
