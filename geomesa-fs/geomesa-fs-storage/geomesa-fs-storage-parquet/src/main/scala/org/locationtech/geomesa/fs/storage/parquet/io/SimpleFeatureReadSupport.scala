/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.conf.ParquetConfiguration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.MessageType
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureReadSupport.SimpleFeatureRecordMaterializer
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.Coordinate

import java.util.{Date, UUID}
import scala.collection.mutable.ArrayBuffer

class SimpleFeatureReadSupport extends ReadSupport[SimpleFeature] {

  private var schema: SimpleFeatureParquetSchema = _

  override def init(context: InitContext): ReadContext = {
    schema = SimpleFeatureParquetSchema.read(context).getOrElse {
      throw new IllegalArgumentException("Could not extract SimpleFeatureType from read context")
    }
    // ensure that our read schema matches the geomesa parquet version
    new ReadContext(schema.schema, schema.metadata)
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

  /**
    * Zip x and y values into coordinates
    *
    * @param x x values
    * @param y corresponding y values
    * @return
    */
  def zip(x: java.util.List[Double], y: java.util.List[Double]): Array[Coordinate] = {
    val result = Array.ofDim[Coordinate](x.size)
    var i = 0
    while (i < result.length) {
      result(i) = new Coordinate(x.get(i), y.get(i))
      i += 1
    }
    result
  }

  class SimpleFeatureRecordMaterializer(schema: SimpleFeatureParquetSchema)
      extends RecordMaterializer[SimpleFeature] {
    private val converter = new SimpleFeatureGroupConverter(schema)
    override def getRootConverter: GroupConverter = converter
    override def getCurrentRecord: SimpleFeature = converter.materialize
  }

  /**
    * Group converter that can create simple features. Note that we should refactor
    * this a little more and perhaps have this store raw values and then push the
    * conversions of SimpleFeature "types" and objects into the SimpleFeatureRecordMaterializer
    * which will mean they are only converted and then added to simple features if a
    * record passes the parquet filters and needs to be materialized.
    */
  class SimpleFeatureGroupConverter(schema: SimpleFeatureParquetSchema) extends GroupConverter {

    private val idConverter = new PrimitiveConverter with ValueMaterializer {
      private var id: Binary = _
      override def materialize(): AnyRef = if (id == null) { null } else { id.toStringUsingUTF8 }
      override def reset(): Unit = id = null
      override def addBinary(value: Binary): Unit = id = value
    }
    private val visConverter = new PrimitiveConverter with ValueMaterializer {
      private var vis: Binary = _
      override def materialize(): AnyRef = if (vis == null) { null } else { vis.toStringUsingUTF8.intern() }
      override def reset(): Unit = vis = null
      override def addBinary(value: Binary): Unit = vis = value
    }

    private val converters = {
      val builder = Array.newBuilder[Converter with ValueMaterializer]
      var i = 0
      while (i < schema.sft.getAttributeCount) {
        builder += SimpleFeatureReadSupport.attribute(ObjectType.selectType(schema.sft.getDescriptor(i)))
        i += 1
      }
      builder += idConverter
      if (schema.hasVisibilities) {
        builder += visConverter
      }
      builder.result()
    }

    // don't materialize unless we have to
    def materialize: SimpleFeature = {
      val id = idConverter.materialize().asInstanceOf[String]
      val vis = visConverter.materialize().asInstanceOf[String]
      val values = Array.tabulate[AnyRef](schema.sft.getAttributeCount)(i => converters(i).materialize())
      val userData = if (vis == null) { null } else {
        val map = new java.util.HashMap[AnyRef, AnyRef](1)
        map.put(SecurityUtils.FEATURE_VISIBILITY, vis)
        map
      }
      new ScalaSimpleFeature(schema.sft, id, values, userData)
    }

    override def start(): Unit = converters.foreach(_.reset())

    override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

    override def end(): Unit = {}
  }

  private def attribute(bindings: Seq[ObjectType]): Converter with ValueMaterializer = {
    bindings.head match {
      case ObjectType.GEOMETRY => geometry(bindings.last)
      case ObjectType.DATE     => new DateConverter()
      case ObjectType.STRING   => new StringConverter()
      case ObjectType.INT      => new IntConverter()
      case ObjectType.DOUBLE   => new DoubleConverter()
      case ObjectType.LONG     => new LongConverter()
      case ObjectType.FLOAT    => new FloatConverter()
      case ObjectType.BOOLEAN  => new BooleanConverter()
      case ObjectType.BYTES    => new BytesConverter()
      case ObjectType.LIST     => new ListConverter(bindings(1))
      case ObjectType.MAP      => new MapConverter(bindings(1), bindings(2))
      case ObjectType.UUID     => new UuidConverter()
      case _ => throw new IllegalArgumentException(s"Can't deserialize field of type ${bindings.head}")
    }
  }

  private def geometry(binding: ObjectType): Converter with ValueMaterializer = {
    binding match {
      case ObjectType.POINT           => new PointConverter()
      case ObjectType.LINESTRING      => new LineStringConverter()
      case ObjectType.POLYGON         => new PolygonConverter()
      case ObjectType.MULTIPOINT      => new MultiPointConverter()
      case ObjectType.MULTILINESTRING => new MultiLineStringConverter()
      case ObjectType.MULTIPOLYGON    => new MultiPolygonConverter()
      case ObjectType.GEOMETRY        => new GeometryWkbConverter()
      case _ => throw new IllegalArgumentException(s"Can't deserialize field of type $binding")
    }
  }

  trait ValueMaterializer {
    def reset(): Unit
    def materialize(): AnyRef
  }

  class DateConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Long = -1
    private var set = false

    override def addLong(value: Long): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): AnyRef = if (set) { new Date(value) } else { null }
  }

  class StringConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Binary = _
    override def reset(): Unit = value = null
    override def materialize(): AnyRef = if (value == null) { null } else { value.toStringUsingUTF8 }
    override def addBinary(value: Binary): Unit = this.value = value
  }

  class IntConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Int = -1
    private var set = false

    override def addInt(value: Int): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): AnyRef = if (set) { Int.box(value) } else { null }
  }

  class LongConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Long = -1
    private var set = false

    override def addLong(value: Long): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): AnyRef = if (set) { Long.box(value) } else { null }
  }

  class FloatConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Float = -1
    private var set = false

    override def addFloat(value: Float): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): AnyRef = if (set) { Float.box(value) } else { null }
  }

  class DoubleConverter extends PrimitiveConverter with ValueMaterializer {
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
    override def materialize(): AnyRef = if (set) { Double.box(value) } else { null }

  }

  class BooleanConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Boolean = false
    private var set = false

    override def addBoolean(value: Boolean): Unit = {
      this.value = value
      set = true
    }
    override def reset(): Unit = set = false
    override def materialize(): AnyRef = if (set) { Boolean.box(value) } else { null }
  }

  class BytesConverter extends PrimitiveConverter with ValueMaterializer {
    private var value: Binary = _
    override def addBinary(value: Binary): Unit = this.value = value
    override def reset(): Unit = value = null
    override def materialize(): AnyRef = if (value == null) { null } else { value.getBytes }
  }

  class ListConverter(binding: ObjectType) extends GroupConverter with ValueMaterializer {

    private var list: java.util.List[AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      private val converter = attribute(Seq(binding))
      override def getConverter(fieldIndex: Int): Converter = converter // better only be one field (0)
      override def start(): Unit = converter.reset()
      override def end(): Unit = list.add(converter.materialize())
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = new java.util.ArrayList[AnyRef]()
    override def end(): Unit = {}
    override def reset(): Unit = list = null
    override def materialize(): AnyRef = list
  }

  class MapConverter(keyBinding: ObjectType, valueBinding: ObjectType) extends GroupConverter with ValueMaterializer {

    private var map: java.util.Map[AnyRef, AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      private val keyConverter = attribute(Seq(keyBinding))
      private val valueConverter = attribute(Seq(valueBinding))

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

  class UuidConverter extends BytesConverter {
    private var value: Binary = _
    override def addBinary(value: Binary): Unit = this.value = value
    override def reset(): Unit = value = null
    override def materialize(): AnyRef = {
      if (value == null) { null } else {
        val bb = value.toByteBuffer
        new UUID(bb.getLong, bb.getLong)
      }
    }
  }

  class PointConverter extends GroupConverter with ValueMaterializer {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new CoordinateConverter()
    private val y = new CoordinateConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {}
    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = gf.createPoint(new Coordinate(x.c, y.c))
  }

  class LineStringConverter extends GroupConverter with ValueMaterializer {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new CoordinateArrayConverter()
    private val y = new CoordinateArrayConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {
      x.i = 0
      y.i = 0
    }

    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = {
      val coords = Array.ofDim[Coordinate](x.i)
      var i = 0
      while (i < coords.length) {
        coords(i) = new Coordinate(x.coords(i), y.coords(i))
        i += 1
      }
      gf.createLineString(coords)
    }
  }

  class MultiPointConverter extends GroupConverter with ValueMaterializer {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new CoordinateArrayConverter()
    private val y = new CoordinateArrayConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {
      x.i = 0
      y.i = 0
    }

    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = {
      val coords = Array.ofDim[Coordinate](x.i)
      var i = 0
      while (i < coords.length) {
        coords(i) = new Coordinate(x.coords(i), y.coords(i))
        i += 1
      }
      gf.createMultiPointFromCoords(coords)
    }
  }

  class PolygonConverter extends GroupConverter with ValueMaterializer {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new LineArrayConverter()
    private val y = new LineArrayConverter()

    override def getConverter(fieldIndex: Int): Converter =  if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {}
    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = {
      val shell = gf.createLinearRing(zip(x.lines.head, y.lines.head))
      val holes = if (x.lines.lengthCompare(1) == 0) { null } else {
        Array.tabulate(x.lines.length - 1)(i => gf.createLinearRing(zip(x.lines(i + 1), y.lines(i + 1))))
      }
      gf.createPolygon(shell, holes)
    }
  }

  class MultiLineStringConverter extends GroupConverter with ValueMaterializer {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new LineArrayConverter()
    private val y = new LineArrayConverter()

    override def getConverter(fieldIndex: Int): Converter =  if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {}
    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = {
      val lines = Array.tabulate(x.lines.length)(i => gf.createLineString(zip(x.lines(i), y.lines(i))))
      gf.createMultiLineString(lines)
    }
  }

  class MultiPolygonConverter extends GroupConverter with ValueMaterializer {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new PolygonArrayConverter()
    private val y = new PolygonArrayConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {}
    override def end(): Unit = {}
    override def reset(): Unit = {}
    override def materialize(): AnyRef = {
      val polys = Array.tabulate(x.polys.length) { i =>
        val shell = gf.createLinearRing(zip(x.polys(i).head, y.polys(i).head))
        val holes = if (x.polys(i).lengthCompare(1) == 0) { null } else {
          Array.tabulate(x.polys(i).length - 1)(j => gf.createLinearRing(zip(x.polys(i)(j + 1), y.polys(i)(j + 1))))
        }
        gf.createPolygon(shell, holes)
      }
      gf.createMultiPolygon(polys)
    }
  }

  class GeometryWkbConverter extends BytesConverter {
    override def materialize(): AnyRef = {
      super.materialize() match {
        case b: Array[Byte] => WKBUtils.read(b)
        case _ => null
      }
    }
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

  /**
    * Primitive converter for handling repeated unboxed double values
    */
  private class CoordinateArrayConverter extends PrimitiveConverter {

    var i: Int = 0
    var coords: Array[Double] = Array.ofDim(16)

    override def addInt(value: Int): Unit = addDouble(value)
    override def addFloat(value: Float): Unit = addDouble(value)
    override def addLong(value: Long): Unit = addDouble(value)
    override def addDouble(value: Double): Unit = {
      if (coords.length == i) {
        val tmp = Array.ofDim[Double](coords.length * 2)
        System.arraycopy(coords, 0, tmp, 0, coords.length)
        coords = tmp
      }
      coords(i) = value
      i += 1
    }
  }

  /**
    * Group converter for handling lists of repeated unboxed double values
    */
  private class LineArrayConverter extends GroupConverter {

    val lines: ArrayBuffer[Array[Double]] = ArrayBuffer.empty

    private val group: GroupConverter = new GroupConverter {
      private val converter = new CoordinateArrayConverter
      override def getConverter(fieldIndex: Int): Converter = converter
      override def start(): Unit = converter.i = 0
      override def end(): Unit = {
        val coords = Array.ofDim[Double](converter.i)
        System.arraycopy(converter.coords, 0, coords, 0, coords.length)
        lines += coords
      }
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = lines.clear()
    override def end(): Unit = {}
  }

  /**
    * Group converter for handling lists of lists of repeated unboxed double values
    */
  private class PolygonArrayConverter extends GroupConverter {

    val polys: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty

    private val group: GroupConverter = new GroupConverter {
      private val converter = new LineArrayConverter
      override def getConverter(fieldIndex: Int): Converter = converter
      override def start(): Unit = {}
      override def end(): Unit = polys += converter.lines.toArray
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = polys.clear()
    override def end(): Unit = {}
  }
}
