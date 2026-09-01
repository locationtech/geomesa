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
import org.apache.parquet.conf.ParquetConfiguration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type}
import org.apache.parquet.variant.{ImmutableMetadata, Variant, VariantBuilder, VariantConverters}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.parquet.io.SimpleFeatureReadSupport.SimpleFeatureRecordMaterializer
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom._

import java.util.function.Consumer
import java.util.{Date, UUID}

class SimpleFeatureReadSupport extends ReadSupport[SimpleFeature] {

  private var schema: SimpleFeatureParquetSchema = _

  override def init(context: InitContext): ReadContext = {
    schema = SimpleFeatureParquetSchema.read(context).getOrElse {
      throw new IllegalArgumentException("Could not extract SimpleFeatureType from read context")
    }
    // ensure that our read schema matches the geomesa parquet version
    new ReadContext(schema.messageType, schema.metadata)
  }

  // noinspection ScalaDeprecation
  override def prepareForRead(
      configuration: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[SimpleFeature] = {
    new SimpleFeatureRecordMaterializer(schema)
  }

  override def prepareForRead(
      configuration: ParquetConfiguration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[SimpleFeature] = {
    new SimpleFeatureRecordMaterializer(schema)
  }
}

object SimpleFeatureReadSupport {

  private val gf = JTSFactoryFinder.getGeometryFactory

  /**
    * Zip x and y values into coordinates
    *
    * @param x x values
    * @param y corresponding y values
    * @return
    */
  def zip(x: Array[Double], y: Array[Double]): Array[Coordinate] = {
    val result = Array.ofDim[Coordinate](x.length)
    var i = 0
    while (i < result.length) {
      result(i) = new Coordinate(x(i), y(i))
      i += 1
    }
    result
  }

  class SimpleFeatureRecordMaterializer(schema: SimpleFeatureParquetSchema)
      extends RecordMaterializer[SimpleFeature] {
    private val converter = new SimpleFeatureGroupConverter(schema)
    override def getRootConverter: GroupConverter = converter
    override def getCurrentRecord: SimpleFeature = converter.materialize()
  }

  /**
    * Group converter that can create simple features
    */
  class SimpleFeatureGroupConverter(schema: SimpleFeatureParquetSchema)
      extends GroupConverter with ValueMaterializer[SimpleFeature] {

    private val idConverter = new StringConverter()

    private val visConverter = new StringConverter() {
      override def materialize(): String = {
        super.materialize() match {
          case null => null
          case s: String => s.intern()
        }
      }
    }

    private val attributes = Array.ofDim[ValueMaterializer[_ <: AnyRef]](schema.sft.getAttributeCount)

    private val converters = {
      val builder = Array.newBuilder[ValueMaterializer[_ <: AnyRef]]
      builder += idConverter
      builder += visConverter
      var i = 0
      var fieldOffset = 2 // 0 is fid, 1 is vis
      while (i < schema.sft.getAttributeCount) {
        val descriptor = schema.sft.getDescriptor(i)
        val types = ObjectType.selectType(descriptor)
        val materializer =
          if (types.head == ObjectType.GEOMETRY) {
            geometry(types.last)
          } else if (types.last == ObjectType.JSON) {
            val groupType = schema.messageType.getType(fieldOffset).asGroupType()
            if (descriptor.getJsonSchema().isDefined) {
              new StructuralJsonConverter(groupType)
            } else {
              new VariantConverter(groupType)
            }
          } else {
            attribute(types)
          }
        builder += materializer
        attributes(i) = materializer
        // note: zValues are excluded from our read schema, they're only used for partitioning
        // note: bboxes have to be present for filtering, but we don't do anything with them on read
        if (types.head == ObjectType.GEOMETRY && fieldOffset + 1 < schema.messageType.getFieldCount &&
            schema.messageType.getFields.get(fieldOffset + 1).getName.startsWith(SimpleFeatureSchema.InternalFieldDelimiter)) {
          builder += new BoundingBoxConverter()
          fieldOffset += 1
        }
        i += 1
        fieldOffset += 1
      }
      builder.result()
    }

    override def reset(): Unit = start()

    override def materialize(): SimpleFeature = {
      val id = idConverter.materialize()
      val vis = visConverter.materialize()
      val values = Array.tabulate[AnyRef](schema.sft.getAttributeCount)(i => attributes(i).materialize())
      val userData = if (vis == null) { null } else {
        val map = new java.util.HashMap[AnyRef, AnyRef](1)
        map.put("geomesa.feature.visibility", vis)
        map
      }
      new ScalaSimpleFeature(schema.sft, id, values, userData)
    }

    override def start(): Unit = converters.foreach(_.reset())

    override def getConverter(fieldIndex: Int): ValueMaterializer[_ <: AnyRef] = converters(fieldIndex)

    override def end(): Unit = {}

    def fieldCount: Int = converters.length

    private def attribute(bindings: Seq[ObjectType]): ValueMaterializer[_ <: AnyRef] = {
      bindings.head match {
        case ObjectType.DATE     => new DateMicrosConverter()
        case ObjectType.STRING   => new StringConverter()
        case ObjectType.INT      => new IntConverter()
        case ObjectType.DOUBLE   => new DoubleConverter()
        case ObjectType.LONG     => new LongConverter()
        case ObjectType.FLOAT    => new FloatConverter()
        case ObjectType.BOOLEAN  => new BooleanConverter()
        case ObjectType.BYTES    => new BytesConverter()
        case ObjectType.LIST     => new ListConverter(attribute(bindings.drop(1)))
        case ObjectType.MAP      => new MapConverter(attribute(bindings.slice(1, 2)), attribute(bindings.slice(2, 3)))
        case ObjectType.UUID     => new UuidConverter()
        case _ => throw new IllegalArgumentException(s"Can't deserialize field of type ${bindings.head}")
      }
    }

    private def geometry(binding: ObjectType): ValueMaterializer[_ <: Geometry] = {
      if (schema.geometries == GeometryEncoding.GeoParquetWkb) {
        new WkbConverter()
      } else if (schema.geometries == GeometryEncoding.GeoParquetNative) {
        binding match {
          case ObjectType.POINT           => new PointConverter()
          case ObjectType.LINESTRING      => new GeoParquetNativeLineStringConverter()
          case ObjectType.POLYGON         => new GeoParquetNativePolygonConverter()
          case ObjectType.MULTIPOINT      => new GeoParquetNativeMultiPointConverter()
          case ObjectType.MULTILINESTRING => new GeoParquetNativeMultiLineStringConverter()
          case ObjectType.MULTIPOLYGON    => new GeoParquetNativeMultiPolygonConverter()
          case _                          => new WkbConverter()
        }
      } else {
        throw new UnsupportedOperationException(s"Can't read geometries encoded with '${schema.geometries}'")
      }
    }
  }

  /**
   * Trait for delaying the materialization of a value
   */
  trait ValueMaterializer[T <: AnyRef] extends Converter {
    def reset(): Unit
    def materialize(): T
  }

  class DateMicrosConverter extends PrimitiveConverter with ValueMaterializer[Date] {
    private var value: Long = -1
    private var set = false

    override def addLong(value: Long): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): Date = if (set) { new Date(value / 1000L) } else { null }
  }

  class StringConverter extends PrimitiveConverter with ValueMaterializer[String] {
    private var value: Binary = _
    override def reset(): Unit = value = null
    override def materialize(): String = if (value == null) { null } else { value.toStringUsingUTF8 }
    override def addBinary(value: Binary): Unit = this.value = value
  }

  private class VariantConverter(schema: GroupType)
      extends GroupConverter with VariantConverters.ParentConverter[VariantBuilder] with ValueMaterializer[String] {

    private var builder: VariantBuilder = _
    private var metadata: ImmutableMetadata = _
    private var value: Variant = _

    private val wrapped = VariantConverters.newVariantConverter(schema, m => this.metadata = new ImmutableMetadata(m), this)

    override def reset(): Unit = value = null
    override def materialize(): String = if (value == null) { null } else { VariantJsonWriter.toJson(value) }
    override def getConverter(fieldIndex: Int): Converter = wrapped.getConverter(fieldIndex)

    override def start(): Unit = {
      builder = null
      metadata = null
      wrapped.start()
    }

    override def end(): Unit = {
      wrapped.end()
      if (builder == null) {
        builder = new VariantBuilder(metadata)
      }
      builder.appendNullIfEmpty()
      value = builder.build()
    }

    override def build(consumer: Consumer[VariantBuilder]): Unit = {
      if (builder == null) {
        builder = new VariantBuilder(metadata)
      }
      consumer.accept(builder)
    }
  }

  class UuidConverter extends PrimitiveConverter with ValueMaterializer[UUID]  {
    private var value: Binary = _
    override def addBinary(value: Binary): Unit = this.value = value
    override def reset(): Unit = value = null
    override def materialize(): UUID = {
      if (value == null) { null } else {
        val bb = value.toByteBuffer
        new UUID(bb.getLong, bb.getLong)
      }
    }
  }

  private class IntConverter extends PrimitiveConverter with ValueMaterializer[Integer] {
    private var value: Int = -1
    private var set = false

    override def addInt(value: Int): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): Integer = if (set) { Int.box(value) } else { null }
  }

  private class LongConverter extends PrimitiveConverter with ValueMaterializer[java.lang.Long] {
    private var value: Long = -1
    private var set = false

    override def addLong(value: Long): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): java.lang.Long = if (set) { Long.box(value) } else { null }
  }

  private class FloatConverter extends PrimitiveConverter with ValueMaterializer[java.lang.Float] {
    private var value: Float = -1
    private var set = false

    override def addFloat(value: Float): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): java.lang.Float = if (set) { Float.box(value) } else { null }
  }

  private class DoubleConverter extends PrimitiveConverter with ValueMaterializer[java.lang.Double] {
    private var value: Double = -1
    private var set = false

    override def addDouble(value: Double): Unit = {
      this.value = value
      set = true
    }
    override def addInt(value: Int): Unit = addDouble(value.toDouble)
    override def addFloat(value: Float): Unit = addDouble(value.toDouble)
    override def addLong(value: Long): Unit = addDouble(value.toDouble)
    override def reset(): Unit = set = false
    override def materialize(): java.lang.Double = if (set) { Double.box(value) } else { null }

  }

  private class BooleanConverter extends PrimitiveConverter with ValueMaterializer[java.lang.Boolean] {
    private var value: java.lang.Boolean = _
    override def addBoolean(value: Boolean): Unit =
      this.value = if (value) { java.lang.Boolean.TRUE } else { java.lang.Boolean.FALSE }
    override def reset(): Unit = value = null
    override def materialize(): java.lang.Boolean = value
  }

  private class BytesConverter extends PrimitiveConverter with ValueMaterializer[Array[Byte]] {
    private var value: Binary = _
    override def addBinary(value: Binary): Unit = this.value = value
    override def reset(): Unit = value = null
    override def materialize(): Array[Byte] = if (value == null) { null } else { value.getBytes }
  }

  class ListConverter(items: ValueMaterializer[_ <: AnyRef])
      extends GroupConverter with ValueMaterializer[java.util.List[AnyRef]] {

    private var list: java.util.List[AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = items
      override def start(): Unit = items.reset()
      override def end(): Unit = list.add(items.materialize())
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = new java.util.ArrayList[AnyRef]()
    override def end(): Unit = {}
    override def reset(): Unit = list = null
    override def materialize(): java.util.List[AnyRef] = list
  }

  class MapConverter(keys: ValueMaterializer[_ <: AnyRef], values: ValueMaterializer[_ <: AnyRef])
      extends GroupConverter with ValueMaterializer[java.util.Map[AnyRef, AnyRef]] {

    private var map: java.util.Map[AnyRef, AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { keys } else { values }
      override def start(): Unit = { keys.reset(); values.reset() }
      override def end(): Unit = map.put(keys.materialize(), values.materialize())
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = map = new java.util.HashMap[AnyRef, AnyRef]()
    override def end(): Unit = {}
    override def reset(): Unit = map = null
    override def materialize(): java.util.Map[AnyRef, AnyRef] = map
  }

  /**
   * Converter for a structural-JSON attribute. Builds a gson tree from the nested parquet group and
   * serializes it back to a compact JSON string, so the value round-trips as a JSON attribute.
   */
  private class StructuralJsonConverter(schema: GroupType)
      extends GroupConverter with ValueMaterializer[String] {

    private val delegate: GroupConverter with JsonElementConverter = jsonGroupConverter(schema)

    override def getConverter(fieldIndex: Int): Converter = delegate.getConverter(fieldIndex)
    override def start(): Unit = delegate.start()
    override def end(): Unit = delegate.end()
    override def reset(): Unit = delegate.reset()
    override def materialize(): String = if (delegate.isSet) { StructuralJson.compact(delegate.element()) } else { null }
  }

  /**
   * A converter that builds up a single gson [[JsonElement]] as its parquet fields are read.
   * `isSet` distinguishes an absent (never-visited) optional field from one that was present.
   */
  private trait JsonElementConverter extends Converter {
    def reset(): Unit
    def isSet: Boolean
    def element(): JsonElement
  }

  private def jsonGroupConverter(tpe: GroupType): GroupConverter with JsonElementConverter = {
    tpe.getLogicalTypeAnnotation match {
      case _: ListLogicalTypeAnnotation => new JsonListConverter(tpe)
      case _: MapLogicalTypeAnnotation  => new JsonMapConverter(tpe)
      case _                            => new JsonRecordConverter(tpe)
    }
  }

  private def jsonConverter(tpe: Type): Converter with JsonElementConverter =
    if (tpe.isPrimitive) { new PrimitiveJsonConverter(tpe.asPrimitiveType()) } else { jsonGroupConverter(tpe.asGroupType()) }

  private class JsonRecordConverter(schema: GroupType) extends GroupConverter with JsonElementConverter {
    private val count = schema.getFieldCount
    private val names = Array.tabulate(count)(i => schema.getFieldName(i))
    private val converters = Array.tabulate(count)(i => jsonConverter(schema.getType(i)))
    private var obj: JsonObject = _
    private var set = false

    override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)
    override def start(): Unit = {
      set = true
      obj = new JsonObject()
      converters.foreach(_.reset())
    }
    override def end(): Unit = {
      var i = 0
      while (i < count) {
        if (converters(i).isSet) {
          obj.add(names(i), converters(i).element())
        }
        i += 1
      }
    }
    override def reset(): Unit = { set = false; obj = null }
    override def isSet: Boolean = set
    override def element(): JsonElement = obj
  }

  private class JsonListConverter(schema: GroupType) extends GroupConverter with JsonElementConverter {
    // standard 3-level list: group (LIST) { repeated group list { <element> } }
    private val elementConverter = jsonConverter(schema.getType(0).asGroupType().getType(0))
    private var array: JsonArray = _
    private var set = false

    private val repeated: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = elementConverter
      override def start(): Unit = elementConverter.reset()
      override def end(): Unit = array.add(if (elementConverter.isSet) { elementConverter.element() } else { JsonNull.INSTANCE })
    }

    override def getConverter(fieldIndex: Int): Converter = repeated
    override def start(): Unit = { set = true; array = new JsonArray() }
    override def end(): Unit = {}
    override def reset(): Unit = { set = false; array = null }
    override def isSet: Boolean = set
    override def element(): JsonElement = array
  }

  private class JsonMapConverter(schema: GroupType) extends GroupConverter with JsonElementConverter {
    // standard map: group (MAP) { repeated group key_value { required <key>; <value> } }
    private val keyValue = schema.getType(0).asGroupType()
    private val keyConverter = jsonConverter(keyValue.getType(0))
    private val valueConverter = jsonConverter(keyValue.getType(1))
    private var obj: JsonObject = _
    private var set = false

    private val entry: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { keyConverter } else { valueConverter }
      override def start(): Unit = { keyConverter.reset(); valueConverter.reset() }
      override def end(): Unit = {
        val key = keyConverter.element() match {
          case p: JsonPrimitive => p.getAsString
          case e => e.toString
        }
        obj.add(key, if (valueConverter.isSet) { valueConverter.element() } else { JsonNull.INSTANCE })
      }
    }

    override def getConverter(fieldIndex: Int): Converter = entry
    override def start(): Unit = { set = true; obj = new JsonObject() }
    override def end(): Unit = {}
    override def reset(): Unit = { set = false; obj = null }
    override def isSet: Boolean = set
    override def element(): JsonElement = obj
  }

  private class PrimitiveJsonConverter(schema: PrimitiveType) extends PrimitiveConverter with JsonElementConverter {
    private val annotation = Option(schema.getLogicalTypeAnnotation)
    private var value: JsonElement = _

    override def isSet: Boolean = value != null
    override def reset(): Unit = value = null
    override def element(): JsonElement = value

    override def addBoolean(v: Boolean): Unit = value = new JsonPrimitive(Boolean.box(v))
    override def addFloat(v: Float): Unit = value = new JsonPrimitive(Float.box(v))
    override def addDouble(v: Double): Unit = value = new JsonPrimitive(Double.box(v))

    override def addInt(v: Int): Unit = value = annotation match {
      case Some(_: DateLogicalTypeAnnotation) => new JsonPrimitive(StructuralJson.dateToJson(v.toLong))
      case Some(t: TimeLogicalTypeAnnotation) => new JsonPrimitive(StructuralJson.timeToJson(v.toLong, t.getUnit))
      case Some(d: DecimalLogicalTypeAnnotation) => new JsonPrimitive(BigDecimal(BigInt(v), d.getScale).bigDecimal)
      case _ => new JsonPrimitive(Int.box(v))
    }

    override def addLong(v: Long): Unit = value = annotation match {
      case Some(t: TimestampLogicalTypeAnnotation) => new JsonPrimitive(StructuralJson.timestampToJson(v, t.getUnit, t.isAdjustedToUTC))
      case Some(t: TimeLogicalTypeAnnotation) => new JsonPrimitive(StructuralJson.timeToJson(v, t.getUnit))
      case Some(d: DecimalLogicalTypeAnnotation) => new JsonPrimitive(BigDecimal(BigInt(v), d.getScale).bigDecimal)
      case _ => new JsonPrimitive(Long.box(v))
    }

    override def addBinary(v: Binary): Unit = value = annotation match {
      case Some(_: StringLogicalTypeAnnotation) => new JsonPrimitive(v.toStringUsingUTF8)
      case Some(_: EnumLogicalTypeAnnotation) => new JsonPrimitive(v.toStringUsingUTF8)
      case Some(d: DecimalLogicalTypeAnnotation) => new JsonPrimitive(BigDecimal(BigInt(v.getBytes), d.getScale).bigDecimal)
      case Some(_: UUIDLogicalTypeAnnotation) =>
        val bb = v.toByteBuffer
        new JsonPrimitive(new UUID(bb.getLong, bb.getLong).toString)
      case _ => new JsonPrimitive(StructuralJson.bytesToJson(v.getBytes))
    }
  }

  private class PointConverter extends GroupConverter with ValueMaterializer[Point] {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new CoordinateConverter()
    private val y = new CoordinateConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {}
    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): Point = gf.createPoint(new Coordinate(x.c, y.c))
  }

  private class GeoParquetNativeLineStringConverter extends GroupConverter with ValueMaterializer[LineString] {

    private val coords = new CoordinateGroupConverter()
    private var list: scala.collection.mutable.ArrayBuilder[Coordinate] = Array.newBuilder[Coordinate]

    private val group: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = coords
      override def start(): Unit = {}
      override def end(): Unit = list += coords.materialize()
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = Array.newBuilder[Coordinate]
    override def end(): Unit = {}
    override def reset(): Unit = list = null
    override def materialize(): LineString = {
      val coords = list.result()
      if (coords.isEmpty) { null } else {
        gf.createLineString(coords)
      }
    }
  }

  private class GeoParquetNativeMultiPointConverter extends GroupConverter with ValueMaterializer[MultiPoint] {
    private val coords = new CoordinateGroupConverter()
    private var list: scala.collection.mutable.ArrayBuilder[Coordinate] = Array.newBuilder[Coordinate]

    private val group: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = coords
      override def start(): Unit = {}
      override def end(): Unit = list += coords.materialize()
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = Array.newBuilder[Coordinate]
    override def end(): Unit = {}
    override def reset(): Unit = list = null
    override def materialize(): MultiPoint = {
      val coords = list.result()
      if (coords.isEmpty) { null } else {
        gf.createMultiPointFromCoords(coords)
      }
    }
  }

  private class GeoParquetNativePolygonConverter extends GroupConverter with ValueMaterializer[Polygon] {

    private val lines = new GeoParquetNativeLineStringConverter()
    private var list: scala.collection.mutable.ArrayBuilder[LineString] = Array.newBuilder[LineString]

    private val group: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = lines
      override def start(): Unit = {}
      override def end(): Unit = list += lines.materialize()
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = Array.newBuilder[LineString]
    override def end(): Unit = {}
    override def reset(): Unit = list = null
    override def materialize(): Polygon = {
      val lines = list.result()
      if (lines.isEmpty) { null } else {
        val shell = gf.createLinearRing(lines.head.getCoordinateSequence)
        val holes = if (lines.lengthCompare(1) == 0) { null } else {
          lines.drop(1).map(line => gf.createLinearRing(line.getCoordinateSequence))
        }
        gf.createPolygon(shell, holes)
      }
    }
  }

  private class GeoParquetNativeMultiLineStringConverter extends GroupConverter with ValueMaterializer[MultiLineString] {

    private val lines = new GeoParquetNativeLineStringConverter()
    private var list: scala.collection.mutable.ArrayBuilder[LineString] = Array.newBuilder[LineString]

    private val group: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = lines
      override def start(): Unit = {}
      override def end(): Unit = list += lines.materialize()
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = Array.newBuilder[LineString]
    override def end(): Unit = {}
    override def reset(): Unit = list = null
    override def materialize(): MultiLineString = {
      val lines = list.result()
      if (lines.isEmpty) { null } else {
        gf.createMultiLineString(lines)
      }
    }
  }

  private class GeoParquetNativeMultiPolygonConverter extends GroupConverter with ValueMaterializer[MultiPolygon] {

    private val polygons = new GeoParquetNativePolygonConverter()
    private var list: scala.collection.mutable.ArrayBuilder[Polygon] = Array.newBuilder[Polygon]

    private val group: GroupConverter = new GroupConverter {
      override def getConverter(fieldIndex: Int): Converter = polygons
      override def start(): Unit = {}
      override def end(): Unit = list += polygons.materialize()
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = Array.newBuilder[Polygon]
    override def end(): Unit = {}
    override def reset(): Unit = list = null
    override def materialize(): MultiPolygon = {
      val polygons = list.result()
      if (polygons.isEmpty) { null } else {
        gf.createMultiPolygon(polygons)
      }
    }
  }

  private class WkbConverter extends PrimitiveConverter with ValueMaterializer[Geometry] {
    private var value: Binary = _
    override def addBinary(value: Binary): Unit = this.value = value
    override def reset(): Unit = value = null
    override def materialize(): Geometry = if (value == null) { null } else { WKBUtils.read(value.getBytes) }
  }

  private class BoundingBoxConverter extends GroupConverter with ValueMaterializer[Array[Float]] {
    private val converters = Array.fill(4)(new FloatConverter())
    override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)
    override def start(): Unit = {}
    override def end(): Unit = {}
    override def reset(): Unit = converters.foreach(_.reset())
    override def materialize(): Array[Float] = converters.map(_.materialize().floatValue())
  }

  /**
    * Primitive converter for reading unboxed double values
    */
  private class CoordinateConverter extends PrimitiveConverter {

    var c: Double = 0.0

    override def addInt(value: Int): Unit = c = value
    override def addFloat(value: Float): Unit = c = value
    override def addLong(value: Long): Unit = c = value
    override def addDouble(value: Double): Unit = c = value
  }

  private class CoordinateGroupConverter extends GroupConverter with ValueMaterializer[Coordinate] {
    private val x = new CoordinateConverter()
    private val y = new CoordinateConverter()
    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }
    override def start(): Unit = {}
    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): Coordinate = new Coordinate(x.c, y.c)
  }
}
