/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.serialization

import com.vividsolutions.jts.geom._

/** A [[DatumWriter]] for [[Geometry]].
  *
  */
trait GeometryWriter[Writer] extends PrimitiveWriter[Writer] with NullableWriter[Writer] {

  // note: dimensions have to be determined from the internal coordinate sequence, not the geometry itself.

  import GeometrySerialization._

  def writeGeometryNotNull(out: Writer, geom: Geometry): Unit =
    geom match {
      case g: Point              => writePoint(out, g)
      case g: LineString         => writeLineString(out, g)
      case g: Polygon            => writePolygon(out, g)
      case g: MultiPoint         => writeGeometryCollection(out, GeometrySerialization.MultiPoint, g)
      case g: MultiLineString    => writeGeometryCollection(out, GeometrySerialization.MultiLineString, g)
      case g: MultiPolygon       => writeGeometryCollection(out, GeometrySerialization.MultiPolygon, g)
      case g: GeometryCollection => writeGeometryCollection(out, GeometrySerialization.GeometryCollection, g)
    }

  private def writePoint(out: Writer, g: Point): Unit = {
    val coords = g.getCoordinateSequence
    val (flag, writeDims) = if (coords.getDimension == 2) { (Point2d, false) } else { (Point, true) }
    writePositiveInt(out, flag)
    writeCoordinateSequence(out, coords, writeLength = false, writeDims)
  }

  private def writeLineString(out: Writer, g: LineString): Unit = {
    val coords = g.getCoordinateSequence
    val (flag, writeDims) = if (coords.getDimension == 2) { (LineString2d, false) } else { (LineString, true) }
    writePositiveInt(out, flag)
    writeCoordinateSequence(out, coords, writeLength = true, writeDims)
  }

  private def writePolygon(out: Writer, g: Polygon): Unit = {
    val exterior = g.getExteriorRing.getCoordinateSequence
    val twoD = exterior.getDimension == 2 &&
        (0 until g.getNumInteriorRing).forall(i => g.getInteriorRingN(i).getCoordinateSequence.getDimension == 2)
    val (flag, writeDims) = if (twoD) { (Polygon2d, false) } else { (Polygon, true) }
    writePositiveInt(out, flag)
    writeCoordinateSequence(out, exterior, writeLength = true, writeDims)
    writePositiveInt(out, g.getNumInteriorRing)
    var i = 0
    while (i < g.getNumInteriorRing) {
      writeCoordinateSequence(out, g.getInteriorRingN(i).getCoordinateSequence, writeLength = true, writeDims)
      i += 1
    }
  }

  private def writeGeometryCollection(out: Writer, typ: Int, g: GeometryCollection): Unit = {
    writePositiveInt(out, typ)
    writePositiveInt(out, g.getNumGeometries)
    var i = 0
    while (i < g.getNumGeometries) {
      writeGeometryNotNull(out, g.getGeometryN(i))
      i += 1
    }
  }

  private def writeCoordinateSequence(out: Writer,
                                      coords: CoordinateSequence,
                                      writeLength: Boolean,
                                      writeDimensions: Boolean): Unit = {
    val dims = coords.getDimension
    if (writeLength) {
      writePositiveInt(out, coords.size())
    }
    if (writeDimensions) {
      writePositiveInt(out, dims)
    }
    var i = 0
    while (i < coords.size()) {
      val coord = coords.getCoordinate(i)
      var j = 0
      while (j < dims) {
        writeDouble(out, coord.getOrdinate(j))
        j += 1
      }
      i += 1
    }
  }


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
  val writeGeometry: DatumWriter[Writer, Geometry] = writeNullable((out, geom) => writeGeometryNotNull(out, geom))
}
