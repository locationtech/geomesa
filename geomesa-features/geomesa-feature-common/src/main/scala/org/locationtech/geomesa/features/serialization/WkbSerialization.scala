/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import org.locationtech.jts.geom._

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

  private lazy val factory = new GeometryFactory()
  private lazy val csFactory = factory.getCoordinateSequenceFactory

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
        case Point2d            => readPoint(in, Some(2))
        case LineString2d       => readLineString(in, Some(2))
        case Polygon2d          => readPolygon(in, Some(2))
        case Point              => readPoint(in, None)
        case LineString         => readLineString(in, None)
        case Polygon            => readPolygon(in, None)
        case MultiPoint         => factory.createMultiPoint(readGeometryCollection[Point](in))
        case MultiLineString    => factory.createMultiLineString(readGeometryCollection[LineString](in))
        case MultiPolygon       => factory.createMultiPolygon(readGeometryCollection[Polygon](in))
        case GeometryCollection => factory.createGeometryCollection(readGeometryCollection[Geometry](in))
        case i => throw new IllegalArgumentException(s"Expected geometry type byte, got $i")
      }
    }
  }

  private def writePoint(out: T, g: Point): Unit = {
    val coords = g.getCoordinateSequence
    val (flag, writeDims) = if (coords.getDimension == 2) { (Point2d, false) } else { (Point, true) }
    out.writeInt(flag, optimizePositive = true)
    writeCoordinateSequence(out, coords, writeLength = false, writeDims)
  }

  private def readPoint(in: V, dims: Option[Int]): Point =
    factory.createPoint(readCoordinateSequence(in, Some(1), dims))

  private def writeLineString(out: T, g: LineString): Unit = {
    val coords = g.getCoordinateSequence
    val (flag, writeDims) = if (coords.getDimension == 2) { (LineString2d, false) } else { (LineString, true) }
    out.writeInt(flag, optimizePositive = true)
    writeCoordinateSequence(out, coords, writeLength = true, writeDims)
  }

  private def readLineString(in: V, dims: Option[Int]): LineString =
    factory.createLineString(readCoordinateSequence(in, None, dims))

  private def writePolygon(out: T, g: Polygon): Unit = {
    val exterior = g.getExteriorRing.getCoordinateSequence
    val twoD = exterior.getDimension == 2 &&
        (0 until g.getNumInteriorRing).forall(i => g.getInteriorRingN(i).getCoordinateSequence.getDimension == 2)
    val (flag, writeDims) = if (twoD) { (Polygon2d, false) } else { (Polygon, true) }
    out.writeInt(flag, optimizePositive = true)
    writeCoordinateSequence(out, exterior, writeLength = true, writeDims)
    out.writeInt(g.getNumInteriorRing, optimizePositive = true)
    var i = 0
    while (i < g.getNumInteriorRing) {
      writeCoordinateSequence(out, g.getInteriorRingN(i).getCoordinateSequence, writeLength = true, writeDims)
      i += 1
    }
  }

  private def readPolygon(in: V, dims: Option[Int]): Polygon = {
    val exteriorRing = factory.createLinearRing(readCoordinateSequence(in, None, dims))
    val numInteriorRings = in.readInt(true)
    if (numInteriorRings == 0) {
      factory.createPolygon(exteriorRing)
    } else {
      val interiorRings = Array.ofDim[LinearRing](numInteriorRings)
      var i = 0
      while (i < numInteriorRings) {
        interiorRings.update(i, factory.createLinearRing(readCoordinateSequence(in, None, dims)))
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

  private def writeCoordinateSequence(out: T,
                                      coords: CoordinateSequence,
                                      writeLength: Boolean,
                                      writeDimensions: Boolean): Unit = {
    val dims = coords.getDimension
    if (writeLength) {
      out.writeInt(coords.size(), optimizePositive = true)
    }
    if (writeDimensions) {
      out.writeInt(dims, optimizePositive = true)
    }
    var i = 0
    while (i < coords.size()) {
      val coord = coords.getCoordinate(i)
      var j = 0
      while (j < dims) {
        out.writeDouble(coord.getOrdinate(j))
        j += 1
      }
      i += 1
    }
  }

  private def readCoordinateSequence(in: V, length: Option[Int], dimensions: Option[Int]): CoordinateSequence = {
    val numCoords = length.getOrElse(in.readInt(true))
    val numDims = dimensions.getOrElse(in.readInt(true))
    val coords = csFactory.create(numCoords, numDims)
    var i = 0
    while (i < numCoords) {
      var j = 0
      while (j < numDims) {
        coords.setOrdinate(i, j, in.readDouble())
        j += 1
      }
      i += 1
    }
    coords
  }
}

object WkbSerialization {

  // 2-d values - corresponds to org.locationtech.jts.io.WKBConstants
  val Point2d: Int            = 1
  val LineString2d: Int       = 2
  val Polygon2d: Int          = 3

  val MultiPoint: Int         = 4
  val MultiLineString: Int    = 5
  val MultiPolygon: Int       = 6
  val GeometryCollection: Int = 7

  // n-dimensional values
  val Point: Int              = 8
  val LineString: Int         = 9
  val Polygon: Int            = 10
}
