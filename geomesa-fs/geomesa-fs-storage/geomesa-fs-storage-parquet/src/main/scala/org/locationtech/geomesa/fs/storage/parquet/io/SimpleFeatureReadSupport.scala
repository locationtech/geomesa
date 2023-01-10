/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.MessageType
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureReadSupport.SimpleFeatureRecordMaterializer
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.Coordinate

import java.nio.ByteBuffer
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

  override def prepareForRead(
      configuration: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadSupport.ReadContext): RecordMaterializer[SimpleFeature] = {
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
    private val converter = new SimpleFeatureGroupConverter(schema.sft)
    override def getRootConverter: GroupConverter = converter
    override def getCurrentRecord: SimpleFeature = converter.materialize
  }

  trait Settable {
    def set(i: Int, value: AnyRef): Unit
  }

  // noinspection LanguageFeature
  implicit def valueToSettable[T](lambda: AnyRef => T): Settable = new Settable {
    override def set(i: Int, value: AnyRef): Unit = lambda(value)
  }

  /**
    * Group converter that can create simple features. Note that we should refactor
    * this a little more and perhaps have this store raw values and then push the
    * conversions of SimpleFeature "types" and objects into the SimpleFeatureRecordMaterializer
    * which will mean they are only converted and then added to simple features if a
    * record passes the parquet filters and needs to be materialized.
    */
  class SimpleFeatureGroupConverter(sft: SimpleFeatureType) extends GroupConverter with Settable {

    // temp placeholders
    private var id: Binary = _
    private val values: Array[AnyRef] = new Array[AnyRef](sft.getAttributeCount)

    private val idConverter = new PrimitiveConverter {
      override def addBinary(value: Binary): Unit = id = value
    }

    private val converters = Array.tabulate(sft.getAttributeCount)(attribute) :+ idConverter

    override def set(idx: Int, value: AnyRef): Unit = values(idx) = value

    // don't materialize unless we have to
    def materialize: SimpleFeature = {
      // deep copy array since the next record may change references in the array
      new ScalaSimpleFeature(sft, id.toStringUsingUTF8, java.util.Arrays.copyOf(values, values.length))
    }

    protected def attribute(i: Int): Converter =
      SimpleFeatureReadSupport.attribute(ObjectType.selectType(sft.getDescriptor(i)), i, this)

    override def start(): Unit = {
      id = null
      var i = 0
      while (i < values.length) {
        values(i) = null
        i += 1
      }
    }

    override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

    override def end(): Unit = {}
  }

  // TODO we are creating lots of objects and boxing primitives here when we may not need to
  // unless a record is materialized so we can likely speed this up by not creating any of
  // the true SFT types util a record passes a filter in the SimpleFeatureRecordMaterializer

  private def attribute(bindings: Seq[ObjectType], i: Int, callback: Settable): Converter = {
    bindings.head match {
      case ObjectType.GEOMETRY => geometry(bindings.last, i, callback)
      case ObjectType.DATE     => new DateConverter(i, callback)
      case ObjectType.STRING   => new StringConverter(i, callback)
      case ObjectType.INT      => new IntConverter(i, callback)
      case ObjectType.DOUBLE   => new DoubleConverter(i, callback)
      case ObjectType.LONG     => new LongConverter(i, callback)
      case ObjectType.FLOAT    => new FloatConverter(i, callback)
      case ObjectType.BOOLEAN  => new BooleanConverter(i, callback)
      case ObjectType.BYTES    => new BytesConverter(i, callback)
      case ObjectType.LIST     => new ListConverter(bindings(1), i, callback)
      case ObjectType.MAP      => new MapConverter(bindings(1), bindings(2), i, callback)
      case ObjectType.UUID     => new UuidConverter(i, callback)
      case _ => throw new IllegalArgumentException(s"Can't deserialize field of type ${bindings.head}")
    }
  }

  private def geometry(binding: ObjectType, i: Int, callback: Settable): Converter = {
    binding match {
      case ObjectType.POINT           => new PointConverter(i, callback)
      case ObjectType.LINESTRING      => new LineStringConverter(i, callback)
      case ObjectType.POLYGON         => new PolygonConverter(i, callback)
      case ObjectType.MULTIPOINT      => new MultiPointConverter(i, callback)
      case ObjectType.MULTILINESTRING => new MultiLineStringConverter(i, callback)
      case ObjectType.MULTIPOLYGON    => new MultiPolygonConverter(i, callback)
      case ObjectType.GEOMETRY        => new GeometryWkbConverter(i, callback)
      case _ => throw new IllegalArgumentException(s"Can't deserialize field of type $binding")
    }
  }

  class DateConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addLong(value: Long): Unit = {
      // TODO this can be optimized to set a long and not materialize date objects
      callback.set(index, new Date(value))
    }
  }

  class StringConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = callback.set(index, value.toStringUsingUTF8)
  }

  class IntConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addInt(value: Int): Unit = callback.set(index, Int.box(value))
  }

  class LongConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addLong(value: Long): Unit = callback.set(index, Long.box(value))
  }

  class FloatConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addFloat(value: Float): Unit = callback.set(index, Float.box(value))
  }

  class DoubleConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addInt(value: Int): Unit = callback.set(index, Double.box(value.toDouble))
    override def addDouble(value: Double): Unit = callback.set(index, Double.box(value))
    override def addFloat(value: Float): Unit = callback.set(index, Double.box(value.toDouble))
    override def addLong(value: Long): Unit = callback.set(index, Double.box(value.toDouble))
  }

  class BooleanConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addBoolean(value: Boolean): Unit = callback.set(index, Boolean.box(value))
  }

  class BytesConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = callback.set(index, value.getBytes)
  }

  class ListConverter(binding: ObjectType, index: Int, callback: Settable) extends GroupConverter {

    private var list: java.util.List[AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      private val converter = attribute(Seq(binding), 0, (value: AnyRef) => list.add(value))
      override def getConverter(fieldIndex: Int): Converter = converter // better only be one field (0)
      override def start(): Unit = {}
      override def end(): Unit = {}
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = list = new java.util.ArrayList[AnyRef]
    override def end(): Unit = callback.set(index, list)
  }

  class MapConverter(keyBinding: ObjectType, valueBinding: ObjectType, index: Int, callback: Settable)
      extends GroupConverter {

    private var map: java.util.Map[AnyRef, AnyRef] = _

    private val group: GroupConverter = new GroupConverter {
      private var k: AnyRef = _
      private var v: AnyRef = _
      private val keyConverter = attribute(Seq(keyBinding), 0, (value: AnyRef) => k = value)
      private val valueConverter = attribute(Seq(valueBinding), 1, (value: AnyRef) => v = value)

      override def getConverter(fieldIndex: Int): Converter =
        if (fieldIndex == 0) { keyConverter } else { valueConverter }

      override def start(): Unit = { k = null; v = null }
      override def end(): Unit = map.put(k, v)
    }

    override def getConverter(fieldIndex: Int): GroupConverter = group
    override def start(): Unit = map = new java.util.HashMap[AnyRef, AnyRef]
    override def end(): Unit = callback.set(index, map)
  }

  class UuidConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      val bb = ByteBuffer.wrap(value.getBytes)
      callback.set(index, new UUID(bb.getLong, bb.getLong))
    }
  }

  class PointConverter(index: Int, callback: Settable) extends GroupConverter {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new CoordinateConverter()
    private val y = new CoordinateConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {
      x.c = 0.0
      y.c = 0.0
    }

    override def end(): Unit = callback.set(index, gf.createPoint(new Coordinate(x.c, y.c)))
  }

  class LineStringConverter(index: Int, callback: Settable) extends GroupConverter {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new CoordinateArrayConverter()
    private val y = new CoordinateArrayConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {
      x.i = 0
      y.i = 0
    }

    override def end(): Unit = {
      val coords = Array.ofDim[Coordinate](x.i)
      var i = 0
      while (i < coords.length) {
        coords(i) = new Coordinate(x.coords(i), y.coords(i))
        i += 1
      }
      callback.set(index, gf.createLineString(coords))
    }
  }

  class MultiPointConverter(index: Int, callback: Settable) extends GroupConverter {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new CoordinateArrayConverter()
    private val y = new CoordinateArrayConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {
      x.i = 0
      y.i = 0
    }

    override def end(): Unit = {
      val coords = Array.ofDim[Coordinate](x.i)
      var i = 0
      while (i < coords.length) {
        coords(i) = new Coordinate(x.coords(i), y.coords(i))
        i += 1
      }
      callback.set(index, gf.createMultiPointFromCoords(coords))
    }
  }

  class PolygonConverter(index: Int, callback: Settable) extends GroupConverter {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new LineArrayConverter()
    private val y = new LineArrayConverter()

    override def getConverter(fieldIndex: Int): Converter =  if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {}

    override def end(): Unit = {
      val shell = gf.createLinearRing(zip(x.lines.head, y.lines.head))
      val holes = if (x.lines.lengthCompare(1) == 0) { null } else {
        Array.tabulate(x.lines.length - 1)(i => gf.createLinearRing(zip(x.lines(i + 1), y.lines(i + 1))))
      }
      callback.set(index, gf.createPolygon(shell, holes))
    }
  }

  class MultiLineStringConverter(index: Int, callback: Settable) extends GroupConverter {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new LineArrayConverter()
    private val y = new LineArrayConverter()

    override def getConverter(fieldIndex: Int): Converter =  if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {}

    override def end(): Unit = {
      val lines = Array.tabulate(x.lines.length)(i => gf.createLineString(zip(x.lines(i), y.lines(i))))
      callback.set(index, gf.createMultiLineString(lines))
    }
  }

  class MultiPolygonConverter(index: Int, callback: Settable) extends GroupConverter {

    private val gf = JTSFactoryFinder.getGeometryFactory

    private val x = new PolygonArrayConverter()
    private val y = new PolygonArrayConverter()

    override def getConverter(fieldIndex: Int): Converter = if (fieldIndex == 0) { x } else { y }

    override def start(): Unit = {}

    override def end(): Unit = {
      val polys = Array.tabulate(x.polys.length) { i =>
        val shell = gf.createLinearRing(zip(x.polys(i).head, y.polys(i).head))
        val holes = if (x.polys(i).lengthCompare(1) == 0) { null } else {
          Array.tabulate(x.polys(i).length - 1)(j => gf.createLinearRing(zip(x.polys(i)(j + 1), y.polys(i)(j + 1))))
        }
        gf.createPolygon(shell, holes)
      }
      callback.set(index, gf.createMultiPolygon(polys))
    }
  }

  class GeometryWkbConverter(index: Int, callback: Settable) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = callback.set(index, WKBUtils.read(value.getBytes))
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
