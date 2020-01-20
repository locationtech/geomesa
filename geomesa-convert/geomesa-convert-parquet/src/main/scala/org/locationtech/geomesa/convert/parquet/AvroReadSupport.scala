/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import java.util.{Collections, Date}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.locationtech.geomesa.convert.parquet.AvroReadSupport.AvroRecordMaterializer
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.parquet.io.SimpleFeatureReadSupport.{BooleanConverter, BytesConverter, DateConverter, DoubleConverter, FloatConverter, IntConverter, LongConverter, Settable, StringConverter}

import scala.collection.mutable.ArrayBuffer

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
    private val root = new GenericGroupConverter(schema.getFields.asScala)
    override def getCurrentRecord: GenericRecord = root.record
    override def getRootConverter: GroupConverter = root
  }

  /**
    * Group converter for a record
    *
    * @param fields fields
    */
  class GenericGroupConverter(fields: Seq[Type]) extends GroupConverter with Settable {

    private val names = fields.map(_.getName).toIndexedSeq
    private val converters = Array.tabulate(fields.length)(i => converter(fields(i), i, this))

    private var rec: AvroRecord = _

    def record: GenericRecord = rec

    override def set(i: Int, value: AnyRef): Unit = rec.put(i, value)

    override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)
    override def start(): Unit = rec = new AvroRecord(names)
    override def end(): Unit = {}
  }

  /**
    * Get a converter for a field type
    *
    * @param field field
    * @param i callback index to set
    * @param callback callback to set result
    * @return
    */
  private def converter(field: Type, i: Int, callback: Settable): Converter = {
    val original = field.getOriginalType
    if (field.isPrimitive) {
      lazy val string = original match {
        case OriginalType.UTF8 => new StringConverter(i, callback)
        case _ => new BytesConverter(i, callback)
      }
      lazy val int32 = original match {
        case OriginalType.DATE => new DaysConverter(i, callback)
        case _ => new IntConverter(i, callback)
      }
      lazy val int64 = original match {
        case OriginalType.TIMESTAMP_MILLIS => new DateConverter(i, callback)
        case OriginalType.TIMESTAMP_MICROS => new MicrosConverter(i, callback)
        case _ => new LongConverter(i, callback)
      }
      field.asPrimitiveType().getPrimitiveTypeName match {
        case PrimitiveTypeName.BINARY               => string
        case PrimitiveTypeName.INT32                => int32
        case PrimitiveTypeName.INT64                => int64
        case PrimitiveTypeName.DOUBLE               => new DoubleConverter(i, callback)
        case PrimitiveTypeName.FLOAT                => new FloatConverter(i, callback)
        case PrimitiveTypeName.BOOLEAN              => new BooleanConverter(i, callback)
        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => new BytesConverter(i, callback)
        case _                                      => NullPrimitiveConverter
      }
    } else {
      val group = field.asGroupType()
      original match {
        case OriginalType.LIST =>
          require(group.getFieldCount == 1 && !group.getType(0).isPrimitive, s"Invalid list type: $group")
          val list = group.getType(0).asGroupType()
          require(list.getFieldCount == 1 && list.isRepetition(Repetition.REPEATED), s"Invalid list type: $group")
          new GenericListConverter(list.getType(0), i, callback)

        case OriginalType.MAP =>
          require(group.getFieldCount == 1 && !group.getType(0).isPrimitive, s"Invalid map type: $group")
          val map = group.getType(0).asGroupType()
          require(map.getFieldCount == 2 && map.isRepetition(Repetition.REPEATED), s"Invalid map type: $group")
          new GenericMapConverter(map.getType(0), map.getType(1), i, callback)

        case _ =>
          if (group.getFields.asScala.forall(t => t.isPrimitive && t.isRepetition(Repetition.REPEATED))) {
            new GroupedRepeatedConverter(group.getFields.asScala.map(_.asPrimitiveType), i, callback)
          } else {
            new GenericGroupConverter(group.getFields.asScala) {
              override def end(): Unit = callback.set(i, record)
            }
          }
      }
    }
  }

  /**
    * Converter for any LIST element, will recursively handle complex list elements
    *
    * @param elements element type
    * @param index callback index to set
    * @param callback callback to set result
    */
  class GenericListConverter(elements: Type, index: Int, callback: Settable) extends GroupConverter {

    import org.locationtech.geomesa.parquet.io.SimpleFeatureReadSupport.valueToSettable

    private var list: java.util.List[AnyRef] = _

    private val group: GroupConverter = if (elements.isPrimitive && elements.isRepetition(Repetition.REPEATED)) {
      new RepeatedConverter(elements.asPrimitiveType(), 0, (value: AnyRef) => list.add(value))
    } else {
      new GroupConverter {
        private val converter = AvroReadSupport.converter(elements, 0, (value: AnyRef) => list.add(value))
        override def getConverter(fieldIndex: Int): Converter = converter // better only be one field (0)
        override def start(): Unit = {}
        override def end(): Unit = {}
      }
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = new java.util.ArrayList[AnyRef]
    override def end(): Unit = callback.set(index, list)
  }

  /**
    * Converter for any MAP element, will recursively handle complex key/value elements
    *
    * @param keys key type
    * @param values value type
    * @param index callback index to set
    * @param callback callback to set result
    */
  class GenericMapConverter(keys: Type, values: Type, index: Int, callback: Settable) extends GroupConverter {

    import org.locationtech.geomesa.parquet.io.SimpleFeatureReadSupport.valueToSettable

    private var map: java.util.Map[AnyRef, AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      private var k: AnyRef = _
      private var v: AnyRef = _
      private val keyConverter = AvroReadSupport.converter(keys, 0, (value: AnyRef) => k = value)
      private val valueConverter = AvroReadSupport.converter(values, 1, (value: AnyRef) => v = value)

      override def getConverter(fieldIndex: Int): Converter =
        if (fieldIndex == 0) { keyConverter } else { valueConverter }

      override def start(): Unit = { k = null; v = null }
      override def end(): Unit = map.put(k, v)
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = map = new java.util.HashMap[AnyRef, AnyRef]
    override def end(): Unit = callback.set(index, map)
  }

  /**
    * Group converter for handling REPEATED fields. Doesn't wrap the field in a record, but just returns
    * a List
    *
    * @param field repeated field type
    * @param index callback index to set
    * @param callback callback to set result
    */
  class RepeatedConverter(field: PrimitiveType, index: Int, callback: Settable)
      extends GroupConverter with Settable {

    private val array = ArrayBuffer.empty[Any]

    private val converter = AvroReadSupport.converter(field, 0, this)

    override def getConverter(fieldIndex: Int): Converter = converter

    override def start(): Unit = array.clear()

    override def end(): Unit = {
      val list = new java.util.ArrayList[Any](array.length)
      array.foreach(list.add)
      callback.set(index, list)
    }

    override def set(ignored: Int, value: AnyRef): Unit = array += value
  }

  /**
    * Group converter for handling groups that consist solely of REPEATED fields. Returns a GenericRecord
    * containing the repeated fields
    *
    * @param fields repeated field types
    * @param index callback index to set
    * @param callback callback to set result
    */
  class GroupedRepeatedConverter(fields: Seq[PrimitiveType], index: Int, callback: Settable)
      extends GroupConverter with Settable {

    private val names = fields.map(_.getName).toIndexedSeq

    private val arrays = Array.fill(fields.length)(ArrayBuffer.empty[Any])

    private val converters = Array.tabulate(fields.length)(i => AvroReadSupport.converter(fields(i), i, this))

    override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

    override def start(): Unit = arrays.foreach(_.clear())

    override def end(): Unit = {
      val rec = new AvroRecord(names)
      var i = 0
      while (i < arrays.length) {
        val list = new java.util.ArrayList[Any](arrays(i).length)
        arrays(i).foreach(list.add)
        rec.put(i, list)
        i += 1
      }
      callback.set(index, rec)
    }

    override def set(i: Int, value: AnyRef): Unit = arrays(i) += value
  }

  /**
    * Converter for a DAYS encoded INT32 field
    *
    * @param index callback index to set
    * @param callback callback to set result
    */
  class DaysConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addInt(value: Int): Unit =
      callback.set(index, Date.from(BinnedTime.Epoch.plusDays(value).toInstant))
  }

  /**
    * Converter for a MICROS encoded INT64 field
    *
    * @param index callback index to set
    * @param callback callback to set result
    */
  class MicrosConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addLong(value: Long): Unit = callback.set(index, new Date(value / 1000L))
  }

  /**
    * Converter for fields that we can't handle - will always return null
    */
  object NullConverter {
    def apply(field: Type): Converter = {
      if (field.isPrimitive) { NullPrimitiveConverter } else {
        val group = field.asGroupType()
        new NullGroupConverter(IndexedSeq.tabulate(group.getFieldCount)(i => apply(group.getType(i))))
      }
    }
  }

  object NullPrimitiveConverter extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {}
    override def addBoolean(value: Boolean): Unit = {}
    override def addDouble(value: Double): Unit = {}
    override def addFloat(value: Float): Unit = {}
    override def addInt(value: Int): Unit = {}
    override def addLong(value: Long): Unit = {}
  }

  class NullGroupConverter(children: IndexedSeq[Converter]) extends GroupConverter {
    override def getConverter(fieldIndex: Int): Converter = children(fieldIndex)
    override def start(): Unit = {}
    override def end(): Unit = {}
  }
}