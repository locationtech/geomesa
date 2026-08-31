/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import com.google.gson._
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.conf.{HadoopParquetConfiguration, ParquetConfiguration}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.{FinalizedWriteContext, WriteContext}
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{GroupType, PrimitiveType, Type}
import org.apache.parquet.variant.{VariantJsonParser, VariantValueWriter}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeoParquetMetadata.GeoParquetObserver
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.{GeometryColumnX, GeometryColumnY, GeometryEncoding}
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName, SimpleFeatureSchema, ZValueField}
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom._

import java.nio.ByteBuffer
import java.util.{Date, UUID}

class SimpleFeatureWriteSupport extends WriteSupport[SimpleFeature] {

  private var writer: SimpleFeatureWriteSupport.SimpleFeatureWriter = _
  private var consumer: RecordConsumer = _
  private var geoParquetObserver: GeoParquetObserver = _
  private var baseMetadata: java.util.Map[String, String] = _

  override val getName: String = "SimpleFeatureWriteSupport"

  // called once
  override def init(conf: Configuration): WriteContext = init(new HadoopParquetConfiguration(conf))

  override def init(conf: ParquetConfiguration): WriteContext = {
    val schema = SimpleFeatureParquetSchema.apply(conf).getOrElse {
      throw new IllegalArgumentException("Could not extract SimpleFeatureType from write context")
    }
    init(schema)
  }

  private def init(schema: SimpleFeatureParquetSchema): WriteContext = {
    this.writer = new SimpleFeatureWriteSupport.SimpleFeatureWriter(schema)
    this.geoParquetObserver = new GeoParquetObserver(schema)
    this.baseMetadata = schema.metadata
    new WriteContext(schema.messageType, schema.metadata)
  }

  // called per block
  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = consumer = recordConsumer

  // called per row
  override def write(record: SimpleFeature): Unit = {
    writer.write(consumer, record)
    geoParquetObserver(record)
  }

  // called once at the end
  override def finalizeWrite(): FinalizedWriteContext = {
    try {
      val metadata = new java.util.HashMap[String, String]()
      metadata.putAll(baseMetadata)
      metadata.putAll(geoParquetObserver.metadata())
      new FinalizedWriteContext(metadata)
    } finally {
      CloseWithLogging(geoParquetObserver)
    }
  }
}

object SimpleFeatureWriteSupport {

  private class SimpleFeatureWriter(schema: SimpleFeatureParquetSchema) {

    private val fids = new FidWriter(0) // ID is the 1st field
    private val vis = new VisibilityWriter(1) // vis is 2nd field

    private val attributes = {
      var index = 2
      Array.tabulate(schema.sft.getAttributeCount) { i =>
        val writer = attribute(schema.sft.getDescriptor(i), index).asInstanceOf[AttributeWriter[AnyRef]]
        index += writer.numFields
        writer
      }
    }

    def write(consumer: RecordConsumer, value: SimpleFeature): Unit = {
      consumer.startMessage()
      fids.apply(consumer, value.getID)
      if (vis != null) {
        vis.apply(consumer, value.getUserData.get("geomesa.feature.visibility").asInstanceOf[String])
      }
      var i = 0
      while (i < attributes.length) {
        attributes(i).apply(consumer, value.getAttribute(i))
        i += 1
      }
      consumer.endMessage()
    }

    private def attribute(descriptor: AttributeDescriptor, index: Int): AttributeWriter[_] = {
      val bindings = ObjectType.selectType(descriptor)
      val col = ColumnName(descriptor.getLocalName)
      if (bindings.head == ObjectType.GEOMETRY) {
        geometry(col, index, bindings.last)
      } else if (bindings.last == ObjectType.JSON) {
        val groupType = schema.messageType.getType(index).asGroupType()
        if (descriptor.getJsonSchema().isDefined) {
          new StructuralJsonWriter(col.column, index, groupType)
        } else {
          new VariantWriter(col.column, index, groupType)
        }
      } else {
        attribute(col.column, index, bindings)
      }
    }

    private def attribute(name: String, index: Int, bindings: Seq[ObjectType]): AttributeWriter[_] = {
      bindings.head match {
        case ObjectType.DATE     => new DateMicrosWriter(name, index)
        case ObjectType.STRING   => new StringWriter(name, index)
        case ObjectType.INT      => new IntegerWriter(name, index)
        case ObjectType.LONG     => new LongWriter(name, index)
        case ObjectType.FLOAT    => new FloatWriter(name, index)
        case ObjectType.DOUBLE   => new DoubleWriter(name, index)
        case ObjectType.BYTES    => new BytesWriter(name, index)
        case ObjectType.LIST     => new ListWriter(name, index, attribute("element", 0, bindings.drop(1)))
        case ObjectType.MAP      => new MapWriter(name, index, attribute("key", 0, bindings.slice(1, 2)), attribute("value", 1, bindings.slice(2, 3)))
        case ObjectType.BOOLEAN  => new BooleanWriter(name, index)
        case ObjectType.UUID     => new UuidWriter(name, index)
        case _ => throw new IllegalArgumentException(s"Can't serialize field '$name' of type ${bindings.head}")
      }
    }

    // TODO support z/m
    private def geometry(col: ColumnName, index: Int, binding: ObjectType): AttributeWriter[_] = {
      if (schema.geometries == GeometryEncoding.GeoParquetWkb) {
        if (binding == ObjectType.POINT) {
          new WkbPointWriter(col.column, index)
        } else {
          new WkbWriter(col.column, index)
        }
      } else {
        binding match {
          case ObjectType.POINT               => new PointWriter(col.column, index)
          case ObjectType.LINESTRING          => new NativeLineStringWriter(col.column, index)
          case ObjectType.POLYGON             => new NativePolygonWriter(col.column, index)
          case ObjectType.MULTIPOINT          => new NativeMultiPointWriter(col.column, index)
          case ObjectType.MULTILINESTRING     => new NativeMultiLineStringWriter(col.column, index)
          case ObjectType.MULTIPOLYGON        => new NativeMultiPolygonWriter(col.column, index)
          case ObjectType.GEOMETRY_COLLECTION => new WkbWriter(col.column, index)
          case ObjectType.GEOMETRY            => new WkbWriter(col.column, index)
          case _ => throw new IllegalArgumentException(s"Can't serialize field '${col.attribute}' of type $binding")
        }
      }
    }
  }

  /**
    * Writes a simple feature attribute to a Parquet file
    */
  private abstract class AttributeWriter[T <: Any](name: String, index: Int, val numFields: Int = 1) {

    /**
      * Writes a value to the current record
      *
      * @param consumer the Parquet record consumer
      * @param value value to write
      */
    def apply(consumer: RecordConsumer, value: T): Unit = {
      if (value != null) {
        consumer.startField(name, index)
        writeFields(consumer, value)
        consumer.endField(name, index)
      }
    }

    def writeFields(consumer: RecordConsumer, value: T): Unit
  }

  private class FidWriter(index: Int) extends AttributeWriter[String](SimpleFeatureSchema.FeatureIdField, index) {
    override def writeFields(consumer: RecordConsumer, value: String): Unit =
      consumer.addBinary(Binary.fromString(value))
  }

  private class VisibilityWriter(index: Int) extends AttributeWriter[String](SimpleFeatureSchema.VisibilitiesField, index) {
    override def writeFields(consumer: RecordConsumer, value: String): Unit =
      consumer.addBinary(Binary.fromString(value))
  }

  private class DateMicrosWriter(name: String, index: Int) extends AttributeWriter[Date](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Date): Unit =
      consumer.addLong(value.getTime * 1000L)
  }

  private class DoubleWriter(name: String, index: Int) extends AttributeWriter[java.lang.Double](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Double): Unit =
      consumer.addDouble(value)
  }

  private class FloatWriter(name: String, index: Int) extends AttributeWriter[java.lang.Float](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Float): Unit =
      consumer.addFloat(value)
  }

  private class IntegerWriter(name: String, index: Int) extends AttributeWriter[java.lang.Integer](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Integer): Unit =
      consumer.addInteger(value)
  }

  private class LongWriter(name: String, index: Int) extends AttributeWriter[java.lang.Long](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Long): Unit =
      consumer.addLong(value)
  }

  private class StringWriter(name: String, index: Int) extends AttributeWriter[String](name, index) {
    override def writeFields(consumer: RecordConsumer, value: String): Unit =
      consumer.addBinary(Binary.fromString(value))
  }

  private class VariantWriter(name: String, index: Int, schema: GroupType) extends AttributeWriter[String](name, index) {
    override def writeFields(consumer: RecordConsumer, value: String): Unit =
      VariantValueWriter.write(consumer, schema, VariantJsonParser.parseJson(value))
  }

  /**
   * Writes a JSON string structurally, according to the parquet schema generated from the attribute's avro schema.
   * The schema uses standard 3-level lists and string-keyed maps (see SimpleFeatureParquetSchema.buildStructuralType).
   */
  private class StructuralJsonWriter(name: String, index: Int, schema: GroupType)
      extends AttributeWriter[String](name, index) {

    override def writeFields(consumer: RecordConsumer, value: String): Unit = {
      val element = JsonParser.parseString(value)
      if (!element.isJsonNull) {
        StructuralJsonWriter.writeValue(consumer, schema, element)
      }
    }
  }

  private object StructuralJsonWriter {

    private def writeValue(consumer: RecordConsumer, tpe: Type, element: JsonElement): Unit = {
      if (tpe.isPrimitive) {
        writePrimitive(consumer, tpe.asPrimitiveType(), element.getAsJsonPrimitive)
      } else {
        val group = tpe.asGroupType()
        group.getLogicalTypeAnnotation match {
          case _: ListLogicalTypeAnnotation => writeList(consumer, group, element.getAsJsonArray)
          case _: MapLogicalTypeAnnotation  => writeMap(consumer, group, element.getAsJsonObject)
          case _                            => writeRecord(consumer, group, element.getAsJsonObject)
        }
      }
    }

    private def writeRecord(consumer: RecordConsumer, group: GroupType, obj: JsonObject): Unit = {
      consumer.startGroup()
      var i = 0
      val fields = group.getFields
      while (i < fields.size()) {
        val field = fields.get(i)
        val fieldName = field.getName
        val value = obj.get(fieldName)
        if (value == null || value.isJsonNull) {
          if (field.isRepetition(Type.Repetition.REQUIRED)) {
            throw new IllegalArgumentException(s"JSON is missing required field '$fieldName'")
          }
        } else {
          consumer.startField(fieldName, i)
          writeValue(consumer, field, value)
          consumer.endField(fieldName, i)
        }
        i += 1
      }
      consumer.endGroup()
    }

    // standard 3-level list: <name> (LIST) { repeated group list { <element>; } }
    private def writeList(consumer: RecordConsumer, group: GroupType, array: JsonArray): Unit = {
      consumer.startGroup()
      if (array.size() > 0) {
        val list = group.getType(0).asGroupType() // repeated 'list' group
        val listName = group.getFieldName(0)
        val element = list.getType(0)
        val elementName = list.getFieldName(0)
        consumer.startField(listName, 0)
        val iter = array.iterator()
        while (iter.hasNext) {
          val item = iter.next()
          consumer.startGroup()
          if (!item.isJsonNull) {
            consumer.startField(elementName, 0)
            writeValue(consumer, element, item)
            consumer.endField(elementName, 0)
          }
          consumer.endGroup()
        }
        consumer.endField(listName, 0)
      }
      consumer.endGroup()
    }

    // string-keyed map: <name> (MAP) { repeated group key_value { required key; value; } }
    private def writeMap(consumer: RecordConsumer, group: GroupType, obj: JsonObject): Unit = {
      consumer.startGroup()
      if (obj.size() > 0) {
        val keyValue = group.getType(0).asGroupType()
        val keyValueName = group.getFieldName(0)
        val keyType = keyValue.getType(0)
        val valueType = keyValue.getType(1)
        consumer.startField(keyValueName, 0)
        val iter = obj.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          consumer.startGroup()
          consumer.startField("key", 0)
          writeValue(consumer, keyType, new JsonPrimitive(entry.getKey))
          consumer.endField("key", 0)
          val v = entry.getValue
          if (v != null && !v.isJsonNull) {
            consumer.startField("value", 1)
            writeValue(consumer, valueType, v)
            consumer.endField("value", 1)
          }
          consumer.endGroup()
        }
        consumer.endField(keyValueName, 0)
      }
      consumer.endGroup()
    }

    private def writePrimitive(consumer: RecordConsumer, tpe: PrimitiveType, value: JsonPrimitive): Unit = {
      val logical = Option(tpe.getLogicalTypeAnnotation)
      tpe.getPrimitiveTypeName match {
        case PrimitiveTypeName.BOOLEAN => consumer.addBoolean(value.getAsBoolean)
        case PrimitiveTypeName.FLOAT   => consumer.addFloat(value.getAsFloat)
        case PrimitiveTypeName.DOUBLE  => consumer.addDouble(value.getAsDouble)

        case PrimitiveTypeName.INT32 =>
          logical match {
            case Some(_: DateLogicalTypeAnnotation) =>
              consumer.addInteger(StructuralJson.jsonToEpochDay(value.getAsString))
            case Some(t: TimeLogicalTypeAnnotation) =>
              consumer.addInteger(StructuralJson.jsonToTime(value.getAsString, t.getUnit).toInt)
            case Some(d: DecimalLogicalTypeAnnotation) =>
              consumer.addInteger(value.getAsBigDecimal.setScale(d.getScale).unscaledValue().intValueExact())
            case _ =>
              consumer.addInteger(value.getAsInt)
          }

        case PrimitiveTypeName.INT64 =>
          logical match {
            case Some(t: TimestampLogicalTypeAnnotation) =>
              consumer.addLong(StructuralJson.jsonToTimestamp(value.getAsString, t.getUnit, t.isAdjustedToUTC))
            case Some(t: TimeLogicalTypeAnnotation) =>
              consumer.addLong(StructuralJson.jsonToTime(value.getAsString, t.getUnit))
            case Some(d: DecimalLogicalTypeAnnotation) =>
              consumer.addLong(value.getAsBigDecimal.setScale(d.getScale).unscaledValue().longValueExact())
            case _ =>
              consumer.addLong(value.getAsLong)
          }

        case PrimitiveTypeName.BINARY =>
          logical match {
            case Some(_: StringLogicalTypeAnnotation) =>
              consumer.addBinary(Binary.fromString(value.getAsString))
            case Some(_: EnumLogicalTypeAnnotation) =>
              consumer.addBinary(Binary.fromString(value.getAsString))
            case _ =>
              consumer.addBinary(Binary.fromConstantByteArray(StructuralJson.jsonToBytes(value.getAsString)))
          }

        case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
          logical match {
            case Some(_: UUIDLogicalTypeAnnotation) =>
              consumer.addBinary(Binary.fromConstantByteArray(StructuralJson.uuidToBytes(UUID.fromString(value.getAsString))))
            case Some(d: DecimalLogicalTypeAnnotation) =>
              val bytes = StructuralJson.decimalToFixedBytes(value.getAsBigDecimal, d.getScale, tpe.getTypeLength)
              consumer.addBinary(Binary.fromConstantByteArray(bytes))
            case _ =>
              consumer.addBinary(Binary.fromConstantByteArray(StructuralJson.jsonToBytes(value.getAsString)))
          }

        case PrimitiveTypeName.INT96 =>
          consumer.addBinary(Binary.fromConstantByteArray(StructuralJson.jsonToBytes(value.getAsString)))
      }
    }
  }

  private class BytesWriter(name: String, index: Int) extends AttributeWriter[Array[Byte]](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Array[Byte]): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(value))
  }

  private class BooleanWriter(name: String, index: Int) extends AttributeWriter[java.lang.Boolean](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Boolean): Unit =
      consumer.addBoolean(value)
  }

  private class ListWriter[T <: Any](name: String, index: Int, elements: AttributeWriter[T])
      extends AttributeWriter[java.util.List[T]](name, index) {

    override def writeFields(consumer: RecordConsumer, value: java.util.List[T]): Unit = {
      consumer.startGroup()
      if (!value.isEmpty) {
        consumer.startField("list", 0)
        consumer.startGroup()
        consumer.startField("element", 0)
        val iter = value.iterator
        while (iter.hasNext) {
          val item = iter.next
          if (item != null) {
            elements.writeFields(consumer, item)
          }
        }
        consumer.endField("element", 0)
        consumer.endGroup()
        consumer.endField("list", 0)
      }
      consumer.endGroup()
    }
  }

  private class MapWriter[U <: Any, V <: Any](name: String, index: Int, keys: AttributeWriter[U], values: AttributeWriter[V])
      extends AttributeWriter[java.util.Map[U, V]](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.util.Map[U, V]): Unit = {
      consumer.startGroup()
      if (!value.isEmpty) {
        consumer.startField("key_value", 0)
        val iter = value.entrySet().iterator
        while (iter.hasNext) {
          val entry = iter.next()
          consumer.startGroup()
          keys(consumer, entry.getKey)
          val v = entry.getValue
          if (v != null) {
            values(consumer, v)
          }
          consumer.endGroup()
        }
        consumer.endField("key_value", 0)
      }
      consumer.endGroup()
    }
  }

  private class UuidWriter(name: String, index: Int) extends AttributeWriter[UUID](name, index) {
    override def writeFields(consumer: RecordConsumer, value: UUID): Unit = {
      val bb = ByteBuffer.wrap(new Array[Byte](16))
      bb.putLong(value.getMostSignificantBits)
      bb.putLong(value.getLeastSignificantBits)
      bb.rewind()
      consumer.addBinary(Binary.fromConstantByteBuffer(bb))
    }
  }

  /**
   * Writes a simple feature attribute to a Parquet file
   */
  private abstract class GeometryWriter[T <: Geometry](name: String, index: Int) extends AttributeWriter[T](name, index, 3) {

    private val bboxCol = BoundingBoxField.groupName(name)

    protected def zCol: String
    protected def z(geom: T): String

    /**
     * Writes a value to the current record
     *
     * @param consumer the Parquet record consumer
     * @param value value to write
     */
    override def apply(consumer: RecordConsumer, value: T): Unit = {
      if (value != null) {
        consumer.startField(name, index)
        writeFields(consumer, value)
        consumer.endField(name, index)
        writeBbox(consumer, value)
        writeZVal(consumer, value)
      }
    }

    private def writeBbox(consumer: RecordConsumer, value: T): Unit = {
      val bbox = value.getEnvelopeInternal
      consumer.startField(bboxCol, index + 1)
      consumer.startGroup()
      consumer.startField(BoundingBoxField.XMin, 0)
      consumer.addFloat(bbox.getMinX.toFloat)
      consumer.endField(BoundingBoxField.XMin, 0)
      consumer.startField(BoundingBoxField.YMin, 1)
      consumer.addFloat(bbox.getMinY.toFloat)
      consumer.endField(BoundingBoxField.YMin, 1)
      consumer.startField(BoundingBoxField.XMax, 2)
      consumer.addFloat(bbox.getMaxX.toFloat)
      consumer.endField(BoundingBoxField.XMax, 2)
      consumer.startField(BoundingBoxField.YMax, 3)
      consumer.addFloat(bbox.getMaxY.toFloat)
      consumer.endField(BoundingBoxField.YMax, 3)
      consumer.endGroup()
      consumer.endField(bboxCol, index + 1)
    }

    private def writeZVal(consumer: RecordConsumer, value: T): Unit = {
      consumer.startField(zCol, index + 2)
      consumer.addBinary(Binary.fromString(z(value)))
      consumer.endField(zCol, index + 2)
    }
  }

  private abstract class GeometryZWriter(name: String, index: Int) extends GeometryWriter[Point](name, index) {
    override protected val zCol: String = ZValueField.z2FieldName(name)
    override protected def z(geom: Point): String = Z2SFC.hexEncode(geom.getX, geom.getY)
  }

  private abstract class GeometryXZWriter[T <: Geometry](name: String, index: Int) extends GeometryWriter[T](name, index) {
    override protected val zCol: String = ZValueField.xz2FieldName(name)
    override protected def z(geom: T): String = {
      val env = geom.getEnvelopeInternal
      XZ2SFC.hexEncode(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    }
  }

  private class PointWriter(name: String, index: Int) extends GeometryZWriter(name, index) {
    override def writeFields(consumer: RecordConsumer, value: Point): Unit = writeFields(consumer, value.getCoordinate)
    def writeFields(consumer: RecordConsumer, value: Coordinate): Unit = {
      consumer.startGroup()
      consumer.startField(GeometryColumnX, 0)
      consumer.addDouble(value.getX)
      consumer.endField(GeometryColumnX, 0)
      consumer.startField(GeometryColumnY, 1)
      consumer.addDouble(value.getY)
      consumer.endField(GeometryColumnY, 1)
      consumer.endGroup()
    }
  }

  private class NativeLineStringWriter(name: String, index: Int) extends GeometryXZWriter[LineString](name, index) {

    private val pointWriter = new PointWriter("", -1)

    override def writeFields(consumer: RecordConsumer, value: LineString): Unit = {
      consumer.startGroup()
      consumer.startField("list", 0)
      consumer.startGroup()
      consumer.startField("element", 0)
      var i = 0
      while (i < value.getNumPoints) {
        val pt = value.getCoordinateN(i)
        pointWriter.writeFields(consumer, pt)
        i += 1
      }
      consumer.endField("element", 0)
      consumer.endGroup()
      consumer.endField("list", 0)
      consumer.endGroup()
    }
  }

  private class NativeMultiPointWriter(name: String, index: Int)
      extends GeometryXZWriter[MultiPoint](name, index) {

    private val pointWriter = new PointWriter("", -1)

    override def writeFields(consumer: RecordConsumer, value: MultiPoint): Unit = {
      consumer.startGroup()
      consumer.startField("list", 0)
      consumer.startGroup()
      consumer.startField("element", 0)
      var i = 0
      while (i < value.getNumGeometries) {
        val pt = value.getGeometryN(i).asInstanceOf[Point]
        pointWriter.writeFields(consumer, pt)
        i += 1
      }
      consumer.endField("element", 0)
      consumer.endGroup()
      consumer.endField("list", 0)
      consumer.endGroup()
    }
  }

  private trait HasLines[T <: Geometry] {
    protected def lines(value: T): Seq[LineString]
  }

  private trait PolygonHasLines extends HasLines[Polygon] {
    override protected def lines(value: Polygon): Seq[LineString] = {
      Seq.tabulate(value.getNumInteriorRing + 1) { i =>
        if (i == 0) { value.getExteriorRing } else { value.getInteriorRingN(i - 1) }
      }
    }
  }

  private trait MultiLineStringHasLines extends HasLines[MultiLineString] {
    override protected def lines(value: MultiLineString): Seq[LineString] =
      Seq.tabulate(value.getNumGeometries)(i => value.getGeometryN(i).asInstanceOf[LineString])
  }

  private abstract class NativeLinesWriter[T <: Geometry](name: String, index: Int)
      extends GeometryXZWriter[T](name, index) with HasLines[T] {

    private val lineWriter = new NativeLineStringWriter(null, -1)

    override def writeFields(consumer: RecordConsumer, value: T): Unit = {
      consumer.startGroup()
      consumer.startField("list", 0)
      consumer.startGroup()
      consumer.startField("element", 0)
      lines(value).foreach { line =>
        lineWriter.writeFields(consumer, line)
      }
      consumer.endField("element", 0)
      consumer.endGroup()
      consumer.endField("list", 0)
      consumer.endGroup()
    }
  }

  private class NativePolygonWriter(name: String, index: Int)
    extends NativeLinesWriter[Polygon](name, index) with PolygonHasLines

  private class NativeMultiLineStringWriter(name: String, index: Int)
      extends NativeLinesWriter[MultiLineString](name, index) with MultiLineStringHasLines

  private class NativeMultiPolygonWriter(name: String, index: Int)
      extends GeometryXZWriter[MultiPolygon](name, index) with PolygonHasLines {

    private val polygonWriter = new NativePolygonWriter("", -1)

    override def writeFields(consumer: RecordConsumer, value: MultiPolygon): Unit = {
      val polys = Seq.tabulate(value.getNumGeometries)(value.getGeometryN(_).asInstanceOf[Polygon])
      consumer.startGroup()
      consumer.startField("list", 0)
      consumer.startGroup()
      consumer.startField("element", 0)
      polys.foreach { poly =>
        polygonWriter.writeFields(consumer, poly)
      }
      consumer.endField("element", 0)
      consumer.endGroup()
      consumer.endField("list", 0)
      consumer.endGroup()
    }
  }

  private class WkbPointWriter(name: String, index: Int) extends GeometryZWriter(name, index) {
    override def writeFields(consumer: RecordConsumer, value: Point): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(WKBUtils.write(value)))
  }

  private class WkbWriter(name: String, index: Int) extends GeometryXZWriter[Geometry](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Geometry): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(WKBUtils.write(value)))
  }
}
