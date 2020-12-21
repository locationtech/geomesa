/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql.jts

import java.io.IOException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts.util.WKBUtils
import org.locationtech.jts.geom._

object JTSTypes {

  val GeometryTypeInstance          : GeometryUDT = GeometryUDT
  val PointTypeInstance             : PointUDT = PointUDT
  val LineStringTypeInstance        : LineStringUDT = LineStringUDT
  val PolygonTypeInstance           : PolygonUDT = PolygonUDT
  val MultiPointTypeInstance        : MultiPointUDT = MultiPointUDT
  val MultiLineStringTypeInstance   : MultiLineStringUDT = MultiLineStringUDT
  @deprecated("replaced with MultiPolygonTypeInstance")
  val MultipolygonTypeInstance      : MultiPolygonUDT = MultiPolygonUDT
  val MultiPolygonTypeInstance      : MultiPolygonUDT = MultiPolygonUDT
  val GeometryCollectionTypeInstance: GeometryCollectionUDT = GeometryCollectionUDT

  // these constant values conform to WKB values
  val GeometryType           = 0
  val PointType              = 1
  val LineStringType         = 2
  val PolygonType            = 3
  val MultiPointType         = 4
  val MultiLineStringType    = 5
  val MultiPolygonType       = 6
  val GeometryCollectionType = 7

  val typeMap: Map[Class[_], Class[_ <: UserDefinedType[_]]] = Map(
    classOf[Geometry]            -> classOf[GeometryUDT],
    classOf[Point]               -> classOf[PointUDT],
    classOf[LineString]          -> classOf[LineStringUDT],
    classOf[Polygon]             -> classOf[PolygonUDT],
    classOf[MultiPoint]          -> classOf[MultiPointUDT],
    classOf[MultiLineString]     -> classOf[MultiLineStringUDT],
    classOf[MultiPolygon]        -> classOf[MultiPolygonUDT],
    classOf[GeometryCollection]  -> classOf[GeometryCollectionUDT]
  )
}

class PointUDT extends AbstractGeometryUDT[Point]("point"){

  // parquet definition:
  // group.id(GeometryBytes.TwkbPoint)
  //  .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
  //  .required(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

  override val sqlType: StructType = StructType(Seq(
    StructField("x", DataTypes.DoubleType, nullable = false),
    StructField("y", DataTypes.DoubleType, nullable = false)
  ))

  override def serialize(obj: Point): InternalRow = {
    if (obj == null) { null } else {
      new GenericInternalRow(Array[Any](obj.getX, obj.getY))
    }
  }

  override def deserialize(datum: Any): Point = {
    if (datum == null) { null } else {
      val row = ir(datum)
      gf.createPoint(new Coordinate(row.getDouble(0), row.getDouble(1)))
    }
  }
}

case object PointUDT extends PointUDT

class MultiPointUDT extends AbstractGeometryUDT[MultiPoint]("multipoint") {

  // parquet definition:
  // case ObjectType.MULTIPOINT =>
  //   group.id(GeometryBytes.TwkbMultiPoint)
  //     .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
  //     .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

  // parquet file metadata:
  //  mpt:          OPTIONAL F:2
  //  .x:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL DOUBLE R:1 D:4
  //  .y:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL DOUBLE R:1 D:4

  override def sqlType: DataType = StructType(Seq(
    StructField("x", CoordArray(DataTypes.DoubleType), nullable = false),
    StructField("y", CoordArray(DataTypes.DoubleType), nullable = false)
  ))

  // TODO seems like these serialization objects will lead to autoboxing of doubles

  override def serialize(obj: MultiPoint): InternalRow = {
    if (obj == null) { null } else {
      val x = new GenericArrayData(Array.ofDim[Any](obj.getNumGeometries))
      val y = new GenericArrayData(Array.ofDim[Any](obj.getNumGeometries))
      var i = 0
      while (i < x.numElements()) {
        val c = obj.getGeometryN(i).asInstanceOf[Point].getCoordinate
        x(i) = c.getX
        y(i) = c.getY
        i += 1
      }
      new GenericInternalRow(Array[Any](x, y))
    }
  }

  override def deserialize(datum: Any): MultiPoint = {
    if (datum == null) { null } else {
      val row = ir(datum)
      val x = row.getArray(0)
      val y = row.getArray(1)
      val c = Array.ofDim[Coordinate](x.numElements())
      var i = 0
      while (i < c.length) {
        c(i) = new Coordinate(x.getDouble(i), y.getDouble(i))
        i += 1
      }
      gf.createMultiPointFromCoords(c)
    }
  }
}

case object MultiPointUDT extends MultiPointUDT

class LineStringUDT extends AbstractGeometryUDT[LineString]("linestring") {

  // parquet definition:
  // case ObjectType.LINESTRING =>
  //   group.id(GeometryBytes.TwkbLineString)
  //     .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnX)
  //     .repeated(PrimitiveTypeName.DOUBLE).named(GeometryColumnY)

  // parquet file metadata:
  //  line:         OPTIONAL F:2
  //  .x:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL DOUBLE R:1 D:4
  //  .y:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL DOUBLE R:1 D:4

  override def sqlType: DataType = StructType(Seq(
    StructField("x", CoordArray(DataTypes.DoubleType), nullable = false),
    StructField("y", CoordArray(DataTypes.DoubleType), nullable = false)
  ))

  override def serialize(obj: LineString): InternalRow = {
    if (obj == null) { null } else {
      val x = new GenericArrayData(Array.ofDim[Any](obj.getNumPoints))
      val y = new GenericArrayData(Array.ofDim[Any](obj.getNumPoints))
      var i = 0
      while (i < x.numElements()) {
        val c = obj.getCoordinateN(i)
        x(i) = c.getX
        y(i) = c.getY
        i += 1
      }
      new GenericInternalRow(Array[Any](x, y))
    }
  }

  override def deserialize(datum: Any): LineString = {
    if (datum == null) { null } else {
      val row = ir(datum)
      val x = row.getArray(0)
      val y = row.getArray(1)
      val c = Array.ofDim[Coordinate](x.numElements())
      var i = 0
      while (i < c.length) {
        c(i) = new Coordinate(x.getDouble(i), y.getDouble(i))
        i += 1
      }
      gf.createLineString(c)
    }
  }
}

case object LineStringUDT extends LineStringUDT

class MultiLineStringUDT extends AbstractGeometryUDT[MultiLineString]("multilinestring") {

  // parquet definition:
  // case ObjectType.MULTILINESTRING =>
  //   group.id(GeometryBytes.TwkbMultiLineString)
  //     .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
  //     .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

  // parquet file metadata:
  //  mline:        OPTIONAL F:2
  //  .x:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL DOUBLE R:1 D:4
  //  .y:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL DOUBLE R:1 D:4

  override def sqlType: DataType = StructType(Seq(
    StructField("x", CoordArray(CoordArray(DataTypes.DoubleType)), nullable = false),
    StructField("y", CoordArray(CoordArray(DataTypes.DoubleType)), nullable = false)
  ))

  override def serialize(obj: MultiLineString): InternalRow = {
    if (obj == null) { null } else {
      val x = new GenericArrayData(Array.ofDim[Any](obj.getNumGeometries))
      val y = new GenericArrayData(Array.ofDim[Any](obj.getNumGeometries))
      var i = 0
      while (i < obj.getNumGeometries) {
        val line = obj.getGeometryN(i).asInstanceOf[LineString]
        val xx = new GenericArrayData(Array.ofDim[Any](line.getNumPoints))
        val yy = new GenericArrayData(Array.ofDim[Any](line.getNumPoints))
        var j = 0
        while (j < xx.numElements()) {
          val c = line.getCoordinateN(j)
          xx(j) = c.getX
          yy(j) = c.getY
          j += 1
        }
        x(i) = xx
        y(i) = yy
        i += 1
      }
      new GenericInternalRow(Array[Any](x, y))
    }
  }

  override def deserialize(datum: Any): MultiLineString = {
    if (datum == null) { null } else {
      val row = ir(datum)
      val x = row.getArray(0)
      val y = row.getArray(1)
      val lines = Array.ofDim[LineString](x.numElements())
      var i = 0
      while (i < lines.length) {
        val xx = x.getArray(i)
        val yy = y.getArray(i)
        val c = Array.ofDim[Coordinate](xx.numElements())
        var j = 0
        while (j < c.length) {
          c(j) = new Coordinate(xx.getDouble(j), yy.getDouble(j))
          j += 1
        }
        lines(i) = gf.createLineString(c)
        i += 1
      }
      gf.createMultiLineString(lines)
    }
  }
}

case object MultiLineStringUDT extends MultiLineStringUDT

class PolygonUDT extends AbstractGeometryUDT[Polygon]("polygon") {

  // parquet definition:
  // case ObjectType.POLYGON =>
  //   group.id(GeometryBytes.TwkbPolygon)
  //     .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
  //     .requiredList().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

  // parquet file metadata:
  //  poly:         OPTIONAL F:2
  //  .x:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL DOUBLE R:1 D:4
  //  .y:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL DOUBLE R:1 D:4

  override def sqlType: DataType = StructType(Seq(
    StructField("x", CoordArray(CoordArray(DataTypes.DoubleType)), nullable = false),
    StructField("y", CoordArray(CoordArray(DataTypes.DoubleType)), nullable = false)
  ))

  override def serialize(obj: Polygon): InternalRow = {
    if (obj == null) { null } else {
      val x = new GenericArrayData(Array.ofDim[Any](obj.getNumInteriorRing + 1))
      val y = new GenericArrayData(Array.ofDim[Any](obj.getNumInteriorRing + 1))
      var i = 0
      while (i < obj.getNumInteriorRing + 1) {
        val line = if (i == 0) { obj.getExteriorRing} else { obj.getInteriorRingN(i - 1) }
        val xx = new GenericArrayData(Array.ofDim[Any](line.getNumPoints))
        val yy = new GenericArrayData(Array.ofDim[Any](line.getNumPoints))
        var j = 0
        while (j < xx.numElements()) {
          val c = line.getCoordinateN(j)
          xx(j) = c.getX
          yy(j) = c.getY
          j += 1
        }
        x(i) = xx
        y(i) = yy
        i += 1
      }
      new GenericInternalRow(Array[Any](x, y))
    }
  }

  override def deserialize(datum: Any): Polygon = {
    if (datum == null) { null } else {
      val row = ir(datum)
      val x = row.getArray(0)
      val y = row.getArray(1)
      var shell: LinearRing = null
      val holes = Array.ofDim[LinearRing](x.numElements() - 1)
      var i = 0
      while (i < x.numElements()) {
        val xx = x.getArray(i)
        val yy = y.getArray(i)
        val c = Array.ofDim[Coordinate](xx.numElements())
        var j = 0
        while (j < c.length) {
          c(j) = new Coordinate(xx.getDouble(j), yy.getDouble(j))
          j += 1
        }
        val line = gf.createLinearRing(c)
        if (i == 0) {
          shell = line
        } else {
          holes(i - 1) = line
        }
        i += 1
      }
      gf.createPolygon(shell, holes)
    }
  }
}

case object PolygonUDT extends PolygonUDT

class MultiPolygonUDT extends AbstractGeometryUDT[MultiPolygon]("multipolygon") {

  // parquet definition:
  // case ObjectType.MULTIPOLYGON =>
  //   group.id(GeometryBytes.TwkbMultiPolygon)
  //     .requiredList().requiredListElement().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnX)
  //     .requiredList().requiredListElement().element(PrimitiveTypeName.DOUBLE, Repetition.REPEATED).named(GeometryColumnY)

  // parquet file metadata:
  //  mpoly:        OPTIONAL F:2
  //  .x:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL F:1
  //  ....list:     REPEATED F:1
  //  .....element: OPTIONAL DOUBLE R:2 D:6
  //  .y:           OPTIONAL F:1
  //  ..list:       REPEATED F:1
  //  ...element:   OPTIONAL F:1
  //  ....list:     REPEATED F:1
  //  .....element: OPTIONAL DOUBLE R:2 D:6

  override def sqlType: DataType = StructType(Seq(
    StructField("x", CoordArray(CoordArray(CoordArray(DataTypes.DoubleType))), nullable = false),
    StructField("y", CoordArray(CoordArray(CoordArray(DataTypes.DoubleType))), nullable = false)
  ))

  override def serialize(obj: MultiPolygon): InternalRow = {
    if (obj == null) { null } else {
      val x = new GenericArrayData(Array.ofDim[Any](obj.getNumGeometries))
      val y = new GenericArrayData(Array.ofDim[Any](obj.getNumGeometries))
      var i = 0
      while (i < obj.getNumGeometries) {
        val poly = obj.getGeometryN(i).asInstanceOf[Polygon]
        val xx = new GenericArrayData(Array.ofDim[Any](poly.getNumInteriorRing + 1))
        val yy = new GenericArrayData(Array.ofDim[Any](poly.getNumInteriorRing + 1))
        var j = 0
        while (j < poly.getNumInteriorRing + 1) {
          val line = if (j == 0) { poly.getExteriorRing} else { poly.getInteriorRingN(j - 1) }
          val xxx = new GenericArrayData(Array.ofDim[Any](line.getNumPoints))
          val yyy = new GenericArrayData(Array.ofDim[Any](line.getNumPoints))
          var k = 0
          while (k < xxx.numElements()) {
            val c = line.getCoordinateN(k)
            xxx(k) = c.getX
            yyy(k) = c.getY
            k += 1
          }
          xx(j) = xxx
          yy(j) = yyy
          j += 1
        }
        x(i) = xx
        y(i) = yy
        i += 1
      }
      new GenericInternalRow(Array[Any](x, y))
    }
  }

  override def deserialize(datum: Any): MultiPolygon = {
    if (datum == null) { null } else {
      val row = ir(datum)
      val x = row.getArray(0)
      val y = row.getArray(1)
      val polys = Array.ofDim[Polygon](x.numElements())
      var i = 0
      while (i < x.numElements()) {
        val xx = x.getArray(i)
        val yy = y.getArray(i)
        var shell: LinearRing = null
        val holes = Array.ofDim[LinearRing](xx.numElements() - 1)
        var j = 0
        while (j < xx.numElements()) {
          val xxx = xx.getArray(j)
          val yyy = yy.getArray(j)
          val c = Array.ofDim[Coordinate](xxx.numElements())
          var k = 0
          while (k < c.length) {
            c(k) = new Coordinate(xxx.getDouble(k), yyy.getDouble(k))
            k += 1
          }
          val line = gf.createLinearRing(c)
          if (j == 0) {
            shell = line
          } else {
            holes(j - 1) = line
          }
          j += 1
        }
        polys(i) = gf.createPolygon(shell, holes)
        i += 1
      }
      gf.createMultiPolygon(polys)
    }
  }
}

case object MultiPolygonUDT extends MultiPolygonUDT

class GeometryUDT extends AbstractGeometryUDT[Geometry]("geometry") {

  // parquet file metadata:
  //  g:    OPTIONAL BINARY R:0 D:1

  private [sql] override def acceptsType(dataType: DataType): Boolean = {
    super.acceptsType(dataType) ||
      dataType.getClass == JTSTypes.GeometryTypeInstance.getClass ||
      dataType.getClass == JTSTypes.PointTypeInstance.getClass ||
      dataType.getClass == JTSTypes.LineStringTypeInstance.getClass ||
      dataType.getClass == JTSTypes.PolygonTypeInstance.getClass ||
      dataType.getClass == JTSTypes.MultiLineStringTypeInstance.getClass ||
      dataType.getClass == JTSTypes.MultiPointTypeInstance.getClass ||
      dataType.getClass == JTSTypes.MultipolygonTypeInstance.getClass ||
      dataType.getClass == JTSTypes.GeometryCollectionTypeInstance.getClass
  }

  // Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
  override def sqlType: DataType = DataTypes.BinaryType

  override def serialize(obj: Geometry): Array[Byte] =
    if (obj == null) { null } else { WKBUtils.write(obj) }

  override def deserialize(datum: Any): Geometry = {
    datum match {
      case null => null
      case a: Array[Byte] => WKBUtils.read(a)
      case _ => throw new IOException(s"Invalid serialized geometry: $datum")
    }
  }
}

case object GeometryUDT extends GeometryUDT

class GeometryCollectionUDT
  extends AbstractGeometryUDT[GeometryCollection]("geometrycollection") {

  private [sql] override def acceptsType(dataType: DataType): Boolean = {
    super.acceptsType(dataType) ||
        dataType.getClass == JTSTypes.MultiLineStringTypeInstance.getClass ||
        dataType.getClass == JTSTypes.MultiPointTypeInstance.getClass ||
        dataType.getClass == JTSTypes.MultipolygonTypeInstance.getClass
  }

  override def sqlType: DataType = DataTypes.BinaryType

  override def serialize(obj: GeometryCollection): Array[Byte] =
    if (obj == null) { null } else { WKBUtils.write(obj) }

  override def deserialize(datum: Any): GeometryCollection = {
    datum match {
      case null => null
      case a: Array[Byte] => WKBUtils.read(a).asInstanceOf[GeometryCollection]
      case _ => throw new IOException(s"Invalid serialized geometry: $datum")
    }
  }
}

case object GeometryCollectionUDT extends GeometryCollectionUDT
