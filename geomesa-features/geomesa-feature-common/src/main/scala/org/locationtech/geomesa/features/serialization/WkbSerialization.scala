/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import org.locationtech.jts.geom._
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory

import scala.reflect.ClassTag

/**
  * Based on the method from geotools WKBWriter. This method is optimized for kryo and simplified from
  * WKBWriter in the following ways:
  *
  * 1. Doesn't save SRID (geomesa didn't use that functionality in WKBWriter)
  * 2. Doesn't worry about byte order (handled by kryo)  TODO does avro handle byte order?
  * 3. Doesn't use a precision model
  */
// noinspection LanguageFeature
trait WkbSerialization[T <: NumericWriter, V <: NumericReader] {

  // note: dimensions have to be determined from the internal coordinate sequence, not the geometry itself.

  import WkbSerialization._

  private val factory = new GeometryFactory()
  private val csFactory = CoordinateArraySequenceFactory.instance()

  private val xySerializer   = new XYSerializer()
  private val xyzSerializer  = new XYZSerializer()
  private val xymSerializer  = new XYMSerializer()
  private val xyzmSerializer = new XYZMSerializer()

  def serializeWkb(out: T, geometry: Geometry): Unit = {
    if (geometry == null) { out.writeByte(NULL_BYTE) } else {
      out.writeByte(NOT_NULL_BYTE)
      geometry match {
        case g: Point              => writePoint(out, g)
        case g: LineString         => writeLineString(out, g)
        case g: Polygon            => writePolygon(out, g)
        case g: MultiPoint         => writeGeometryCollection(out, WkbSerialization.MultiPoint, g)
        case g: MultiLineString    => writeGeometryCollection(out, WkbSerialization.MultiLineString, g)
        case g: MultiPolygon       => writeGeometryCollection(out, WkbSerialization.MultiPolygon, g)
        case g: GeometryCollection => writeGeometryCollection(out, WkbSerialization.GeometryCollection, g)
      }
    }
  }

  def deserializeWkb(in: V, checkNull: Boolean = false): Geometry = {
    if (checkNull && in.readByte() == NULL_BYTE) { null } else {
      in.readInt(true) match {
        case Point              => readPoint(in, xySerializer)
        case LineString         => readLineString(in, xySerializer)
        case Polygon            => readPolygon(in, xySerializer)
        case PointXYZ           => readPoint(in, xyzSerializer)
        case LineStringXYZ      => readLineString(in, xyzSerializer)
        case PolygonXYZ         => readPolygon(in, xyzSerializer)
        case PointXYM           => readPoint(in, xymSerializer)
        case LineStringXYM      => readLineString(in, xymSerializer)
        case PolygonXYM         => readPolygon(in, xymSerializer)
        case PointXYZM          => readPoint(in, xyzmSerializer)
        case LineStringXYZM     => readLineString(in, xyzmSerializer)
        case PolygonXYZM        => readPolygon(in, xyzmSerializer)
        case MultiPoint         => factory.createMultiPoint(readGeometryCollection[Point](in))
        case MultiLineString    => factory.createMultiLineString(readGeometryCollection[LineString](in))
        case MultiPolygon       => factory.createMultiPolygon(readGeometryCollection[Polygon](in))
        case GeometryCollection => factory.createGeometryCollection(readGeometryCollection[Geometry](in))
        // legacy encodings - dimension serialized as a separate int
        case 8                  => readLegacyPoint(in)
        case 9                  => readLegacyLineString(in)
        case 10                 => readLegacyPolygon(in)
        case f => throw new IllegalArgumentException(s"Expected geometry type byte, but got $f")
      }
    }
  }

  private def writePoint(out: T, g: Point): Unit = {
    val writer = getWriter(g)
    out.writeInt(writer.flag + Point, optimizePositive = true)
    writeCoordinateSequence(out, writer, g.getCoordinateSequence, writeLength = false)
  }

  private def readPoint(in: V, reader: CoordinateSerializer): Point =
    factory.createPoint(readCoordinateSequence(in, reader, Some(1)))

  private def readLegacyPoint(in: V): Point = {
    in.readInt(optimizePositive = true) match {
      case 2 => readPoint(in, xySerializer)
      case 3 => readPoint(in, xyzSerializer)
      case i => throw new IllegalArgumentException(s"Expected 2 or 3 dimensions, but got $i")
    }
  }

  private def writeLineString(out: T, g: LineString): Unit = {
    val writer = getWriter(g)
    out.writeInt(writer.flag + LineString, optimizePositive = true)
    val coords = g.getCoordinateSequence
    writeCoordinateSequence(out, writer, coords, writeLength = true)
  }

  private def readLineString(in: V, reader: CoordinateSerializer): LineString =
    factory.createLineString(readCoordinateSequence(in, reader, None))

  private def readLegacyLineString(in: V): LineString = {
    val length = in.readInt(optimizePositive = true)
    in.readInt(optimizePositive = true) match {
      case 2 => factory.createLineString(readCoordinateSequence(in, xySerializer, Some(length)))
      case 3 => factory.createLineString(readCoordinateSequence(in, xyzSerializer, Some(length)))
      case i => throw new IllegalArgumentException(s"Expected 2 or 3 dimensions, but got $i")
    }
  }

  private def writePolygon(out: T, g: Polygon): Unit = {
    val writer = getWriter(g)
    out.writeInt(writer.flag + Polygon, optimizePositive = true)
    val exterior = g.getExteriorRing.getCoordinateSequence
    writeCoordinateSequence(out, writer, exterior, writeLength = true)
    out.writeInt(g.getNumInteriorRing, optimizePositive = true)
    var i = 0
    while (i < g.getNumInteriorRing) {
      writeCoordinateSequence(out, writer, g.getInteriorRingN(i).getCoordinateSequence, writeLength = true)
      i += 1
    }
  }

  private def readPolygon(in: V, reader: CoordinateSerializer): Polygon = {
    val exteriorRing = factory.createLinearRing(readCoordinateSequence(in, reader, None))
    val numInteriorRings = in.readInt(true)
    if (numInteriorRings == 0) {
      factory.createPolygon(exteriorRing)
    } else {
      val interiorRings = Array.ofDim[LinearRing](numInteriorRings)
      var i = 0
      while (i < numInteriorRings) {
        interiorRings(i) = factory.createLinearRing(readCoordinateSequence(in, reader, None))
        i += 1
      }
      factory.createPolygon(exteriorRing, interiorRings)
    }
  }

  private def readLegacyPolygon(in: V): Polygon = {
    def readLinearRing(): LinearRing = {
      val length = in.readInt(optimizePositive = true)
      in.readInt(optimizePositive = true) match {
        case 2 => factory.createLinearRing(readCoordinateSequence(in, xySerializer, Some(length)))
        case 3 => factory.createLinearRing(readCoordinateSequence(in, xyzSerializer, Some(length)))
        case i => throw new IllegalArgumentException(s"Expected 2 or 3 dimensions, but got $i")
      }
    }
    val exteriorRing = readLinearRing()
    val numInteriorRings = in.readInt(true)
    if (numInteriorRings == 0) {
      factory.createPolygon(exteriorRing)
    } else {
      val interiorRings = Array.ofDim[LinearRing](numInteriorRings)
      var i = 0
      while (i < numInteriorRings) {
        interiorRings(i) = readLinearRing()
        i += 1
      }
      factory.createPolygon(exteriorRing, interiorRings)
    }
  }

  private def writeGeometryCollection(out: T, typ: Int, g: GeometryCollection): Unit = {
    out.writeInt(typ, optimizePositive = true)
    out.writeInt(g.getNumGeometries, optimizePositive = true)
    var i = 0
    while (i < g.getNumGeometries) {
      serializeWkb(out, g.getGeometryN(i))
      i += 1
    }
  }

  private def readGeometryCollection[U <: Geometry: ClassTag](in: V): Array[U] = {
    val numGeoms = in.readInt(true)
    val geoms = Array.ofDim[U](numGeoms)
    var i = 0
    while (i < numGeoms) {
      geoms.update(i, deserializeWkb(in, checkNull = true).asInstanceOf[U])
      i += 1
    }
    geoms
  }

  private def writeCoordinateSequence(
      out: T,
      writer: CoordinateSerializer,
      coords: CoordinateSequence,
      writeLength: Boolean): Unit = {
    if (writeLength) {
      out.writeInt(coords.size(), optimizePositive = true)
    }
    var i = 0
    while (i < coords.size()) {
      writer.write(out, coords.getCoordinate(i))
      i += 1
    }
  }

  private def readCoordinateSequence(in: V, reader: CoordinateSerializer, length: Option[Int]): CoordinateSequence = {
    val numCoords = length.getOrElse(in.readInt(true))
    val coords = Array.ofDim[Coordinate](numCoords)
    var i = 0
    while (i < numCoords) {
      coords(i) = reader.read(in)
      i += 1
    }
    csFactory.create(coords)
  }

  private def getWriter(geometry: Geometry): CoordinateSerializer = {
    // don't trust coord.getDimensions - it always returns 3 in jts
    // instead, check for NaN for the z dimension
    // note that we only check the first coordinate - if a geometry is written with different
    // dimensions in each coordinate, some information may be lost
    val coord = geometry.getCoordinate
    if (coord == null) { xySerializer } else {
      // check for dimensions - use NaN != NaN to verify coordinate dimensions
      val hasZ = !java.lang.Double.isNaN(coord.getZ)
      val hasM = !java.lang.Double.isNaN(coord.getM)
      if (hasZ) {
        if (hasM) {
          xyzmSerializer
        } else {
          xyzSerializer
        }
      } else if (hasM) {
        xymSerializer
      } else {
        xySerializer
      }
    }
  }

  private sealed trait CoordinateSerializer {
    def flag: Int
    def write(out: T, coord: Coordinate): Unit
    def read(in: V): Coordinate
  }

  private class XYSerializer extends CoordinateSerializer {
    override val flag: Int = XYFlag
    override def write(out: T, coord: Coordinate): Unit = {
      out.writeDouble(coord.getX)
      out.writeDouble(coord.getY)
    }
    override def read(in: V): Coordinate = {
      val x = in.readDouble()
      val y = in.readDouble()
      new CoordinateXY(x, y)
    }
  }

  private class XYZSerializer extends CoordinateSerializer {
    override val flag: Int = XYZFlag
    override def write(out: T, coord: Coordinate): Unit = {
      out.writeDouble(coord.getX)
      out.writeDouble(coord.getY)
      out.writeDouble(coord.getZ)
    }
    override def read(in: V): Coordinate = {
      val x = in.readDouble()
      val y = in.readDouble()
      val z = in.readDouble()
      new Coordinate(x, y, z)
    }
  }

  private class XYMSerializer extends CoordinateSerializer {
    override val flag: Int = XYMFlag
    override def write(out: T, coord: Coordinate): Unit = {
      out.writeDouble(coord.getX)
      out.writeDouble(coord.getY)
      out.writeDouble(coord.getM)
    }
    override def read(in: V): Coordinate = {
      val x = in.readDouble()
      val y = in.readDouble()
      val m = in.readDouble()
      new CoordinateXYM(x, y, m)
    }
  }

  private class XYZMSerializer extends CoordinateSerializer {
    override val flag: Int = XYZMFlag
    override def write(out: T, coord: Coordinate): Unit = {
      out.writeDouble(coord.getX)
      out.writeDouble(coord.getY)
      out.writeDouble(coord.getZ)
      out.writeDouble(coord.getM)
    }
    override def read(in: V): Coordinate = {
      val x = in.readDouble()
      val y = in.readDouble()
      val z = in.readDouble()
      val m = in.readDouble()
      new CoordinateXYZM(x, y, z, m)
    }
  }
}

object WkbSerialization {

  // geometry type constants
  val Point              : Int = 1
  val LineString         : Int = 2
  val Polygon            : Int = 3
  val MultiPoint         : Int = 4
  val MultiLineString    : Int = 5
  val MultiPolygon       : Int = 6
  val GeometryCollection : Int = 7

  // dimension constants
  val XYFlag   : Int = 0
  val XYZFlag  : Int = 1000
  val XYMFlag  : Int = 2000
  val XYZMFlag : Int = 3000

  // used in our match statement
  val PointXYZ       : Int = Point      + XYZFlag
  val LineStringXYZ  : Int = LineString + XYZFlag
  val PolygonXYZ     : Int = Polygon    + XYZFlag
  val PointXYM       : Int = Point      + XYMFlag
  val LineStringXYM  : Int = LineString + XYMFlag
  val PolygonXYM     : Int = Polygon    + XYMFlag
  val PointXYZM      : Int = Point      + XYZMFlag
  val LineStringXYZM : Int = LineString + XYZMFlag
  val PolygonXYZM    : Int = Polygon    + XYZMFlag
}
