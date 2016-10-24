/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.serialization

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKBConstants

import scala.reflect.ClassTag

/**
  * Based on the method from geotools WKBWriter. This method is optimized for kryo and simplified from
  * WKBWriter in the following ways:
  *
  * 1. Doesn't save SRID (geomesa didn't use that functionality in WKBWriter)
  * 2. Doesn't handle dimensions > 2
  * 3. Doesn't worry about byte order (handled by kryo)  TODO does avro handle byte order?
  * 4. Doesn't use a precision model
  *
  */
// noinspection LanguageFeature
trait GeometrySerialization[T <: NumericWriter, V <: NumericReader] extends LazyLogging {

  private lazy val factory = new GeometryFactory()
  private lazy val csFactory = factory.getCoordinateSequenceFactory

  def serialize(out: T, geometry: Geometry): Unit = {
    geometry match {
      case g: Point =>
        out.writeInt(WKBConstants.wkbPoint, optimizePositive = true)
        writeCoordinate(out, g.getCoordinateSequence.getCoordinate(0))

      case g: LineString =>
        out.writeInt(WKBConstants.wkbLineString, optimizePositive = true)
        writeCoordinateSequence(out, g.getCoordinateSequence)

      case g: Polygon => writePolygon(out, g)

      case g: MultiPoint => writeGeometryCollection(out, WKBConstants.wkbMultiPoint, g)

      case g: MultiLineString => writeGeometryCollection(out, WKBConstants.wkbMultiLineString, g)

      case g: MultiPolygon => writeGeometryCollection(out, WKBConstants.wkbMultiPolygon, g)

      case g: GeometryCollection => writeGeometryCollection(out, WKBConstants.wkbGeometryCollection, g)
    }
  }

  def deserialize(in: V): Geometry = {
    in.readInt(true) match {
      case WKBConstants.wkbPoint => factory.createPoint(readCoordinate(in))

      case WKBConstants.wkbLineString => factory.createLineString(readCoordinateSequence(in))

      case WKBConstants.wkbPolygon => readPolygon(in)

      case WKBConstants.wkbMultiPoint =>
        val geoms = readGeometryCollection[Point](in)
        factory.createMultiPoint(geoms)

      case WKBConstants.wkbMultiLineString =>
        val geoms = readGeometryCollection[LineString](in)
        factory.createMultiLineString(geoms)

      case WKBConstants.wkbMultiPolygon =>
        val geoms = readGeometryCollection[Polygon](in)
        factory.createMultiPolygon(geoms)

      case WKBConstants.wkbGeometryCollection =>
        val geoms = readGeometryCollection[Geometry](in)
        factory.createGeometryCollection(geoms)
    }
  }

  private def writeGeometryCollection(out: T, typ: Int, g: GeometryCollection): Unit = {
    out.writeInt(typ, optimizePositive = true)
    out.writeInt(g.getNumGeometries, optimizePositive = true)
    var i = 0
    while (i < g.getNumGeometries) {
      serialize(out, g.getGeometryN(i))
      i += 1
    }
  }

  private def readGeometryCollection[U <: Geometry: ClassTag](in: V): Array[U] = {
    val numGeoms = in.readInt(true)
    val geoms = Array.ofDim[U](numGeoms)
    var i = 0
    while (i < numGeoms) {
      geoms.update(i, deserialize(in).asInstanceOf[U])
      i += 1
    }
    geoms
  }

  private def writePolygon(out: T, g: Polygon): Unit = {
    out.writeInt(WKBConstants.wkbPolygon, optimizePositive = true)
    writeCoordinateSequence(out, g.getExteriorRing.getCoordinateSequence)
    out.writeInt(g.getNumInteriorRing, optimizePositive = true)
    var i = 0
    while (i < g.getNumInteriorRing) {
      writeCoordinateSequence(out, g.getInteriorRingN(i).getCoordinateSequence)
      i += 1
    }
  }

  private def readPolygon(in: V): Polygon = {
    val exteriorRing = factory.createLinearRing(readCoordinateSequence(in))
    val numInteriorRings = in.readInt(true)
    if (numInteriorRings == 0) {
      factory.createPolygon(exteriorRing)
    } else {
      val interiorRings = Array.ofDim[LinearRing](numInteriorRings)
      var i = 0
      while (i < numInteriorRings) {
        interiorRings.update(i, factory.createLinearRing(readCoordinateSequence(in)))
        i += 1
      }
      factory.createPolygon(exteriorRing, interiorRings)
    }
  }

  private def writeCoordinateSequence(out: T, coords: CoordinateSequence): Unit = {
    out.writeInt(coords.size(), optimizePositive = true)
    var i = 0
    while (i < coords.size()) {
      writeCoordinate(out, coords.getCoordinate(i))
      i += 1
    }
  }

  private def readCoordinateSequence(in: V): CoordinateSequence = {
    val numCoords = in.readInt(true)
    val coords = csFactory.create(numCoords, 2)
    var i = 0
    while (i < numCoords) {
      coords.setOrdinate(i, 0, in.readDouble())
      coords.setOrdinate(i, 1, in.readDouble())
      i += 1
    }
    coords
  }

  private def writeCoordinate(out: T, coord: Coordinate): Unit = {
    out.writeDouble(coord.getOrdinate(0))
    out.writeDouble(coord.getOrdinate(1))
  }

  private def readCoordinate(in: V): CoordinateSequence = {
    val coords = csFactory.create(1, 2)
    coords.setOrdinate(0, 0, in.readDouble())
    coords.setOrdinate(0, 1, in.readDouble())
    coords
  }
}
