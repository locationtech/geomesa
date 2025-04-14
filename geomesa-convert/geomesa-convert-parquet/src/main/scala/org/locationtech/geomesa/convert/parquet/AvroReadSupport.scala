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

import java.util.{Collections, Date}

/**
  * Read support for parsing an arbitrary parquet file into avro records.
  *
  * The official parquet-avro reader doesn't support 'repeated' columns, and requires avro 1.8,
  * so we roll our own
  */
class AvroReadSupport extends ReadSupport[GenericRecord] {

  override def init(context: InitContext): ReadContext =
    new ReadContext(context.getFileSchema, Collections.emptyMap())

  override def prepareForRead(
      configuration: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[GenericRecord] = {
    new AvroRecordMaterializer(fileSchema)
  }

  override def prepareForRead(
    configuration: ParquetConfiguration,
    keyValueMetaData: java.util.Map[String, String],
    fileSchema: MessageType,
    readContext: ReadContext): RecordMaterializer[GenericRecord] = {
    new AvroRecordMaterializer(fileSchema)
  }
}

object AvroReadSupport {

  import scala.collection.JavaConverters._

  /**
    * Schema-less implementation of GenericRecord
    *
    * @param fields field names
    */
  class AvroRecord(fields: IndexedSeq[String]) extends GenericRecord {

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

  class AvroRecordMaterializer(schema: MessageType) extends RecordMaterializer[GenericRecord] {
    private val root = new GenericGroupConverter(schema.getFields.asScala.toSeq)
    override def getCurrentRecord: GenericRecord = root.record
    override def getRootConverter: GroupConverter = root
  }

  /**
    * Group converter for a record
    *
    * @param fields fields
    */
  class GenericGroupConverter(fields: Seq[Type]) extends GroupConverter with ValueMaterializer {

    private val names = fields.map(_.getName).toIndexedSeq
    private val converters = Array.tabulate(fields.length)(i => converter(fields(i)))

    private var rec: AvroRecord = _

    def record: GenericRecord = rec

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
    override def materialize(): AnyRef = rec
  }

  /**
    * Get a converter for a field type
    *
    * @param field field
    * @return
    */
  private def converter(field: Type): Converter with ValueMaterializer = {
    val logical = field.getLogicalTypeAnnotation
    if (field.isPrimitive) {
      lazy val stringOrBytes = logical match {
        case _: StringLogicalTypeAnnotation => new StringConverter()
        case _ => new BytesConverter()
      }
      lazy val intOrDate = logical match {
        case _: DateLogicalTypeAnnotation => new DaysConverter()
        case _ => new IntConverter()
      }
      lazy val longOrTimestamp = logical match {
        case t: TimestampLogicalTypeAnnotation if t.getUnit == LogicalTypeAnnotation.TimeUnit.MILLIS => new DateConverter()
        case t: TimestampLogicalTypeAnnotation if t.getUnit == LogicalTypeAnnotation.TimeUnit.MICROS => new MicrosConverter()
        case _ => new LongConverter()
      }
      val convert = field.asPrimitiveType().getPrimitiveTypeName match {
        case PrimitiveTypeName.BINARY               => stringOrBytes
        case PrimitiveTypeName.INT32                => intOrDate
        case PrimitiveTypeName.INT64                => longOrTimestamp
        case PrimitiveTypeName.DOUBLE               => new DoubleConverter()
        case PrimitiveTypeName.FLOAT                => new FloatConverter()
        case PrimitiveTypeName.BOOLEAN              => new BooleanConverter()
        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => new BytesConverter()
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
          new GenericListConverter(list.getType(0))

        case _: MapLogicalTypeAnnotation =>
          require(group.getFieldCount == 1 && !group.getType(0).isPrimitive, s"Invalid map type: $group")
          val map = group.getType(0).asGroupType()
          require(map.getFieldCount == 2 && map.isRepetition(Repetition.REPEATED), s"Invalid map type: $group")
          new GenericMapConverter(map.getType(0), map.getType(1))

        case _ =>
          new GenericGroupConverter(group.getFields.asScala.toSeq)
      }
    }
  }

  /**
    * Converter for any LIST element, will recursively handle complex list elements
    *
    * @param elements element type
    */
  class GenericListConverter(elements: Type) extends GroupConverter with ValueMaterializer {

    private var list: java.util.List[AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      private val converter = AvroReadSupport.converter(elements)
      override def getConverter(fieldIndex: Int): Converter = converter // better only be one field (0)
      override def start(): Unit = converter.reset()
      override def end(): Unit = list.add(converter.materialize())
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = new java.util.ArrayList[AnyRef]
    override def end(): Unit = {}
    override def reset(): Unit = list = null
    override def materialize(): AnyRef = list
  }

  /**
    * Converter for any MAP element, will recursively handle complex key/value elements
    *
    * @param keys key type
    * @param values value type
    */
  class GenericMapConverter(keys: Type, values: Type) extends GroupConverter with ValueMaterializer {

    private var map: java.util.Map[AnyRef, AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      private val keyConverter = AvroReadSupport.converter(keys)
      private val valueConverter = AvroReadSupport.converter(values)

      override def getConverter(fieldIndex: Int): Converter =
        if (fieldIndex == 0) { keyConverter } else { valueConverter }

      override def start(): Unit = { keyConverter.reset(); valueConverter.reset() }
      override def end(): Unit = map.put(keyConverter.materialize(), valueConverter.materialize())
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = map = new java.util.HashMap[AnyRef, AnyRef]()
    override def end(): Unit = {}
    override def reset(): Unit = map = null
    override def materialize(): AnyRef = map
  }

  /**
   * Converter for primitive fields with repetition of 'repeated'
   *
   * @param delegate single value converter
   */
  private class RepeatedPrimitiveConverter(delegate: PrimitiveConverter with ValueMaterializer)
    extends PrimitiveConverter with ValueMaterializer {

    private var list: java.util.List[AnyRef] = _

    override def addBinary(value: Binary): Unit = { delegate.asPrimitiveConverter().addBinary(value); addElement() }
    override def addBoolean(value: Boolean): Unit = { delegate.asPrimitiveConverter().addBoolean(value); addElement() }
    override def addInt(value: Int): Unit = { delegate.asPrimitiveConverter().addInt(value); addElement() }
    override def addFloat(value: Float): Unit = { delegate.asPrimitiveConverter().addFloat(value); addElement() }
    override def addLong(value: Long): Unit = { delegate.asPrimitiveConverter().addLong(value); addElement() }
    override def addDouble(value: Double): Unit = { delegate.asPrimitiveConverter().addDouble(value); addElement() }

    private def addElement(): Unit = {
      if (list == null) {
        list = new java.util.ArrayList[AnyRef]()
      }
      list.add(delegate.materialize())
      delegate.reset()
    }
    override def reset(): Unit = list = null
    override def materialize(): AnyRef = list
  }

  /**
    * Converter for a DAYS encoded INT32 field
    */
  class DaysConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Int = -1
    private var set = false

    override def addInt(value: Int): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): AnyRef = if (set) { Date.from(BinnedTime.Epoch.plusDays(value).toInstant) } else { null }
  }

  /**
    * Converter for a MICROS encoded INT64 field
    */
  class MicrosConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Long = -1
    private var set = false

    override def addLong(value: Long): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): AnyRef = if (set) { new Date(value / 1000L) } else { null }
  }

  object NullPrimitiveConverter extends PrimitiveConverter with ValueMaterializer {
    override def addBinary(value: Binary): Unit = {}
    override def addBoolean(value: Boolean): Unit = {}
    override def addDouble(value: Double): Unit = {}
    override def addFloat(value: Float): Unit = {}
    override def addInt(value: Int): Unit = {}
    override def addLong(value: Long): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = null
  }

  class NullGroupConverter(children: IndexedSeq[Converter]) extends GroupConverter with ValueMaterializer {
    override def getConverter(fieldIndex: Int): Converter = children(fieldIndex)
    override def start(): Unit = {}
    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = null
  }
}
