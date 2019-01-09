/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.geometry.GeometryPrecision.TwkbPrecision
import org.locationtech.jts.geom._

import scala.util.control.NonFatal

/**
  * Based on the TWKB standard: https://github.com/TWKB/Specification/blob/master/twkb.md
  *
  * For backwards compatibility, also reads original serialization, with the `LegacyGeometrySerialization` trait
  */
// noinspection LanguageFeature
trait TwkbSerialization[T <: NumericWriter, V <: NumericReader]
    extends VarIntEncoding[T, V] with WkbSerialization[T, V] with LazyLogging {

  import DimensionalBounds._
  import TwkbSerialization.FlagBytes._
  import TwkbSerialization.GeometryBytes._
  import TwkbSerialization.ZeroByte

  private val factory = new GeometryFactory()
  private val csFactory = factory.getCoordinateSequenceFactory

  /**
    * Serialize a geometry
    *
    * For explanation of precisions, see `org.locationtech.geomesa.utils.geometry.GeometryPrecision`
    *
    * @param out output
    * @param geometry geometry
    * @param precision precision for encoding x, y, z, m
    */
  def serialize(out: T, geometry: Geometry, precision: TwkbPrecision = TwkbPrecision()): Unit = {
    if (geometry == null) {
      out.writeByte(ZeroByte)
    } else {
      // choose our state to correspond with the dimensions in the geometry
      implicit val state: DeltaState = {
        // note that we only check the first coordinate - if a geometry is written with different
        // dimensions in each coordinate, some information may be lost
        val coord = geometry.getCoordinate
        // check for dimensions - use NaN != NaN to verify z coordinate
        // TODO check for M coordinate when added to JTS
        if (coord == null || java.lang.Double.isNaN(coord.getZ)) {
          new XYState(precision.xy)
        } else {
          new XYZState(precision.xy, precision.z)
        }
      }

      geometry match {
        case g: Point =>
          if (g.isEmpty) {
            state.writeMetadata(out, TwkbPoint, empty = true, bbox = false)
          } else {
            state.writeMetadata(out, TwkbPoint, empty = false, bbox = false)
            state.writeCoordinate(out, g.getCoordinate)
          }

        case g: LineString =>
          if (g.isEmpty) {
            state.writeMetadata(out, TwkbLineString, empty = true, bbox = false)
          } else {
            state.writeMetadata(out, TwkbLineString, empty = false, bbox = true)
            state.writeBoundingBox(out, g)
          }
          writeLineString(out, g)

        case g: Polygon =>
          if (g.isEmpty) {
            state.writeMetadata(out, TwkbPolygon, empty = true, bbox = false)
          } else {
            state.writeMetadata(out, TwkbPolygon, empty = false, bbox = true)
            state.writeBoundingBox(out, g)
          }
          writePolygon(out, g)

        case g: MultiPoint =>
          if (g.isEmpty) {
            state.writeMetadata(out, TwkbMultiPoint, empty = true, bbox = false)
          } else {
            state.writeMetadata(out, TwkbMultiPoint, empty = false, bbox = true)
            state.writeBoundingBox(out, g)
          }
          writeMultiPoint(out, g)

        case g: MultiLineString =>
          if (g.isEmpty) {
            state.writeMetadata(out, TwkbMultiLineString, empty = true, bbox = false)
          } else {
            state.writeMetadata(out, TwkbMultiLineString, empty = false, bbox = true)
            state.writeBoundingBox(out, g)
          }
          writeMultiLineString(out, g)

        case g: MultiPolygon =>
          if (g.isEmpty) {
            state.writeMetadata(out, TwkbMultiPolygon, empty = true, bbox = false)
          } else {
            state.writeMetadata(out, TwkbMultiPolygon, empty = false, bbox = true)
            state.writeBoundingBox(out, g)
          }
          writeMultiPolygon(out, g)

        case g: GeometryCollection =>
          if (g.isEmpty) {
            state.writeMetadata(out, TwkbCollection, empty = true, bbox = false)
          } else {
            state.writeMetadata(out, TwkbCollection, empty = false, bbox = true)
            state.writeBoundingBox(out, g)
          }
          writeCollection(out, g)
      }
    }
  }

  /**
    * Deserialize a geometry
    *
    * @param in input
    * @return
    */
  def deserialize(in: V): Geometry = {
    try {
      val precisionAndType = in.readByte()
      if (precisionAndType == ZeroByte) {
        null
      } else if (precisionAndType == NOT_NULL_BYTE) {
        // TODO this overlaps with twkb point type with precision 0
        deserializeWkb(in)
      } else {
        // first byte contains the geometry type in the first 4 bits and the x-y precision in the second 4 bits
        val geomType = (precisionAndType & 0x0F).toByte
        val precision = VarIntEncoding.zigzagDecode((precisionAndType & 0xF0) >>> 4)

        // second byte contains flags for optional elements
        val flags = in.readByte()
        val hasBoundingBox = (flags & BoundingBoxFlag) != 0
        val hasExtendedDims = (flags & ExtendedDimsFlag) != 0
        val isEmpty = (flags & EmptyFlag) != 0

        // extended dims indicates the presence of z and/or m
        // we create our state tracker based on the dimensions that are present
        implicit val state: DeltaState = if (hasExtendedDims) {
          // z and m precisions are indicated in the next byte, where (from right to left):
          //   bit 0 indicates presence of z dimension
          //   bit 1 indicates presence of m dimension
          //   bits 2-5 indicate z precision
          //   bits 6-8 indicate m precision
          val extendedDims = in.readByte()
          if ((extendedDims & 0x01) != 0) { // indicates z dimension
            if ((extendedDims & 0x02) != 0) { // indicates m dimension
              new XYZMState(precision, (extendedDims & 0x1C) >> 2, (extendedDims & 0xE0) >>> 5)
            } else {
              new XYZState(precision, (extendedDims & 0x1C) >> 2)
            }
          } else if ((extendedDims & 0x02) != 0) {  // indicates m dimension
            new XYMState(precision, (extendedDims & 0xE0) >>> 5)
          } else {
            // not sure why anyone would indicate extended dims but set them all false...
            new XYState(precision)
          }
        } else {
          new XYState(precision)
        }

        // size is the length of the remainder of the geometry, after the size attribute
        // we don't currently use size - parsing will fail if size is actually present

        // val hasSize = (flags & FlagBytes.SizeFlag) != 0
        // if (hasSize) {
        //   val size = readUnsignedVarInt(in)
        // }

        // bounding box is not currently used, but we write it in anticipation of future filter optimizations
        if (hasBoundingBox) {
          state.skipBoundingBox(in)
        }

        // children geometries can be written with an id list
        // we don't currently use ids - parsing will fail if ids are actually present
        // val hasIds = (flags & FlagBytes.IdsFlag) != 0

        geomType match {
          case TwkbPoint => factory.createPoint(if (isEmpty) { null } else { csFactory.create(readPointArray(in, 1)) })
          case TwkbLineString      => readLineString(in)
          case TwkbPolygon         => readPolygon(in)
          case TwkbMultiPoint      => readMultiPoint(in)
          case TwkbMultiLineString => readMultiLineString(in)
          case TwkbMultiPolygon    => readMultiPolygon(in)
          case TwkbCollection      => readCollection(in)
          case _ => throw new IllegalArgumentException(s"Invalid TWKB geometry type $geomType")
        }
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error reading serialized kryo geometry:", e); null
    }
  }

  private def writeLineString(out: T, g: LineString)(implicit state: DeltaState): Unit =
      writePointArray(out, g.getCoordinateSequence, g.getNumPoints)

  private def readLineString(in: V)(implicit state: DeltaState): LineString =
    factory.createLineString(csFactory.create(readPointArray(in, readUnsignedVarInt(in))))

  private def writePolygon(out: T, g: Polygon)(implicit state: DeltaState): Unit = {
    if (g.isEmpty) {
      writeUnsignedVarInt(out, 0)
    } else {
      val numRings = g.getNumInteriorRing
      writeUnsignedVarInt(out, numRings + 1) // include exterior ring in count
      // note: don't write final point for each ring, as they should duplicate the first point
      var ring = g.getExteriorRing.getCoordinateSequence
      writePointArray(out, ring, ring.size() - 1)
      var j = 0
      while (j < numRings) {
        ring = g.getInteriorRingN(j).getCoordinateSequence
        writePointArray(out, ring, ring.size() - 1)
        j += 1
      }
    }
  }

  private def readPolygon(in: V)(implicit state: DeltaState): Polygon = {
    val numRings = readUnsignedVarInt(in)
    if (numRings == 0) { factory.createPolygon(null, null) } else {
      val exteriorRing = readLinearRing(in, readUnsignedVarInt(in))
      val interiorRings = Array.ofDim[LinearRing](numRings - 1)
      var i = 1
      while (i < numRings) {
        interiorRings(i - 1) = readLinearRing(in, readUnsignedVarInt(in))
        i += 1
      }
      factory.createPolygon(exteriorRing, interiorRings)
    }
  }

  private def writeMultiPoint(out: T, g: MultiPoint)(implicit state: DeltaState): Unit = {
    val length = g.getNumPoints
    writeUnsignedVarInt(out, length)
    var i = 0
    while (i < length) {
      state.writeCoordinate(out, g.getGeometryN(i).asInstanceOf[Point].getCoordinate)
      i += 1
    }
  }

  private def readMultiPoint(in: V)(implicit state: DeltaState): MultiPoint = {
    val numPoints = readUnsignedVarInt(in)
    if (numPoints == 0) { factory.createMultiPoint(null: CoordinateSequence) } else {
      // note: id list would go here, with one ID per point
      factory.createMultiPoint(readPointArray(in, numPoints).map(factory.createPoint))
    }
  }

  private def writeMultiLineString(out: T, g: MultiLineString)(implicit state: DeltaState): Unit = {
    val length = g.getNumGeometries
    writeUnsignedVarInt(out, length)
    var i = 0
    while (i < length) {
      val line = g.getGeometryN(i).asInstanceOf[LineString].getCoordinateSequence
      writePointArray(out, line, line.size())
      i += 1
    }
  }

  private def readMultiLineString(in: V)(implicit state: DeltaState): MultiLineString = {
    val numLineStrings = readUnsignedVarInt(in)
    if (numLineStrings == 0) { factory.createMultiLineString(null) } else {
      // note: id list would go here, with one ID per line string
      val lineStrings = Array.ofDim[LineString](numLineStrings)
      var i = 0
      while (i < numLineStrings) {
        lineStrings(i) = readLineString(in)
        i += 1
      }
      factory.createMultiLineString(lineStrings)
    }
  }

  private def writeMultiPolygon(out: T, g: MultiPolygon)(implicit state: DeltaState): Unit = {
    val length = g.getNumGeometries
    writeUnsignedVarInt(out, length)
    var i = 0
    while (i < length) {
      writePolygon(out, g.getGeometryN(i).asInstanceOf[Polygon])
      i += 1
    }
  }

  private def readMultiPolygon(in: V)(implicit state: DeltaState): MultiPolygon = {
    val numPolygons = readUnsignedVarInt(in)
    if (numPolygons == 0) { factory.createMultiPolygon(null) } else {
      // note: id list would go here, with one ID per polygon
      val polygons = Array.ofDim[Polygon](numPolygons)
      var i = 0
      while (i < numPolygons) {
        polygons(i) = readPolygon(in)
        i += 1
      }
      factory.createMultiPolygon(polygons)
    }
  }

  private def writeCollection(out: T, g: GeometryCollection)(implicit state: DeltaState): Unit = {
    val length = g.getNumGeometries
    writeUnsignedVarInt(out, length)
    var i = 0
    while (i < length) {
      serialize(out, g.getGeometryN(i))
      i += 1
    }
  }

  private def readCollection(in: V): GeometryCollection = {
    val numGeoms = readUnsignedVarInt(in)
    if (numGeoms == 0) { factory.createGeometryCollection(null) } else {
      // note: id list would go here, with one ID per sub geometry
      val geoms = Array.ofDim[Geometry](numGeoms)
      var i = 0
      while (i < numGeoms) {
        geoms(i) = deserialize(in)
        i += 1
      }
      factory.createGeometryCollection(geoms)
    }
  }

  private def writePointArray(out: T, coords: CoordinateSequence, length: Int)(implicit state: DeltaState): Unit = {
    writeUnsignedVarInt(out, length)
    var i = 0
    while (i < length) {
      state.writeCoordinate(out, coords.getCoordinate(i))
      i += 1
    }
  }

  private def readPointArray(in: V, length: Int)(implicit state: DeltaState): Array[Coordinate] = {
    val result = Array.ofDim[Coordinate](length)
    var i = 0
    while (i < length) {
      result(i) = state.readCoordinate(in)
      i += 1
    }
    result
  }

  private def readLinearRing(in: V, length: Int)(implicit state: DeltaState): LinearRing = {
    if (length == 0) { factory.createLinearRing(null: CoordinateSequence) } else {
      val result = Array.ofDim[Coordinate](length + 1)
      var i = 0
      while (i < length) {
        result(i) = state.readCoordinate(in)
        i += 1
      }
      // linear rings should not store the final, duplicate point, but still need it for the geometry
      result(length) = result(0)
      factory.createLinearRing(csFactory.create(result))
    }
  }

  /**
    * TWKB only reads and writes the delta from one coordinate to the next, which generally saves space
    * over absolute values. This trait tracks the values used for delta calculations over a single
    * read or write operation
    */
  private sealed trait DeltaState {

    /**
      * Write metadata, which includes the geometry and precision byte, the flag byte, and optionally
      * an extended precision byte
      *
      * @param out output
      * @param geometryType geometry type
      * @param empty indicate that the geometry is empty
      * @param bbox indicate that a bbox will be written
      */
    def writeMetadata(out: T, geometryType: Byte, empty: Boolean, bbox: Boolean): Unit

    /**
      * Writes out a bounding box. Each dimension stores a min value and a delta to the max value
      *
      * @param out output
      * @param geometry geometry
      * @param bounds bounds operation
      * @tparam G geometry type
      */
    def writeBoundingBox[G <: Geometry](out: T, geometry: G)(implicit bounds: DimensionalBounds[G]): Unit

    /**
      * Skips over a bounding box. We don't currently use the bounding box when reading
      *
      * @param in in
      */
    def skipBoundingBox(in: V): Unit

    /**
      * Write a coordinate
      *
      * @param out output
      * @param coordinate coordinate
      */
    def writeCoordinate(out: T, coordinate: Coordinate): Unit

    /**
      * Read a coordinate
      *
      * @param in input
      * @return
      */
    def readCoordinate(in: V): Coordinate

    /**
      * Reset the state back to its original state, suitable for re-use
      */
    def reset(): Unit
  }

  private class XYState(precision: Int) extends DeltaState {

    private val p: Double = math.pow(10, precision)
    private var x: Int = 0
    private var y: Int = 0

    protected val boundingBoxFlag: Byte = BoundingBoxFlag
    protected val emptyFlag: Byte = EmptyFlag
    protected val dimensionsFlag: Byte = ZeroByte

    override def writeMetadata(out: T, geometryType: Byte, empty: Boolean, bbox: Boolean): Unit = {
      // write the geometry type and the main precision
      out.writeByte(((VarIntEncoding.zigzagEncode(precision) << 4) | geometryType).toByte)
      // write the flag byte
      out.writeByte(if (bbox) { boundingBoxFlag } else if (empty) { emptyFlag } else { dimensionsFlag })
    }

    override def writeCoordinate(out: T, coordinate: Coordinate): Unit = {
      val cx = math.round(coordinate.x * p).toInt
      val cy = math.round(coordinate.y * p).toInt
      writeVarInt(out, cx - x)
      writeVarInt(out, cy - y)
      x = cx
      y = cy
    }

    override def readCoordinate(in: V): Coordinate = {
      x = x + readVarInt(in)
      y = y + readVarInt(in)
      new Coordinate(x / p, y / p)
    }

    override def writeBoundingBox[G <: Geometry](out: T, geometry: G)(implicit bounds: DimensionalBounds[G]): Unit = {
      val (minX, maxX) = bounds.x(geometry)
      val intX = math.round(minX * p).toInt
      writeVarInt(out, intX)
      writeVarInt(out, math.round(maxX * p).toInt - intX)
      val (minY, maxY) = bounds.y(geometry)
      val intY = math.round(minY * p).toInt
      writeVarInt(out, intY)
      writeVarInt(out, math.round(maxY * p).toInt - intY)
    }

    override def skipBoundingBox(in: V): Unit = {
      skipVarInt(in)
      skipVarInt(in)
      skipVarInt(in)
      skipVarInt(in)
    }

    override def reset(): Unit = {
      x = 0
      y = 0
    }
  }

  private abstract class ExtendedState(precision: Int) extends XYState(precision) {

    protected def extendedDims: Byte

    override protected val boundingBoxFlag: Byte = (ExtendedDimsFlag | BoundingBoxFlag).toByte
    override protected val emptyFlag: Byte = (ExtendedDimsFlag | EmptyFlag).toByte
    override protected val dimensionsFlag: Byte = ExtendedDimsFlag

    override def writeMetadata(out: T, geometryType: Byte, empty: Boolean, bbox: Boolean): Unit = {
      super.writeMetadata(out, geometryType, empty, bbox)
      // write the extended precision values
      out.writeByte(extendedDims)
    }
  }

  private class XYZState(precision: Int, zPrecision: Int) extends ExtendedState(precision) {

    private val pz: Double = math.pow(10, zPrecision)
    private var z: Int = 0

    // sets bits for z dim, and its precisions
    override protected val extendedDims: Byte = (0x01 | ((zPrecision & 0x03) << 2)).toByte

    override def writeBoundingBox[G <: Geometry](out: T, geometry: G)(implicit bounds: DimensionalBounds[G]): Unit = {
      super.writeBoundingBox(out, geometry)
      val (minZ, maxZ) = bounds.z(geometry)
      val intZ = math.round(minZ * pz).toInt
      writeVarInt(out, intZ)
      writeVarInt(out, math.round(maxZ * pz).toInt - intZ)
    }

    override def writeCoordinate(out: T, coordinate: Coordinate): Unit = {
      super.writeCoordinate(out, coordinate)
      val cz = math.round(coordinate.getZ * pz).toInt
      writeVarInt(out, cz - z)
      z = cz
    }

    override def readCoordinate(in: V): Coordinate = {
      val coord = super.readCoordinate(in)
      z = z + readVarInt(in)
      coord.setZ(z / pz)
      coord
    }

    override def skipBoundingBox(in: V): Unit = {
      super.skipBoundingBox(in)
      skipVarInt(in)
      skipVarInt(in)
    }

    override def reset(): Unit = {
      super.reset()
      z = 0
    }
  }

  private class XYMState(precision: Int, mPrecision: Int) extends ExtendedState(precision) {

    private val pm: Double = math.pow(10, mPrecision)
    private var m: Int = 0

    // sets bit for m dim, and its precisions
    override protected val extendedDims: Byte = (0x02 | ((mPrecision & 0x03) << 5)).toByte

    override def writeBoundingBox[G <: Geometry](out: T, geometry: G)(implicit bounds: DimensionalBounds[G]): Unit = {
      super.writeBoundingBox(out, geometry)
      val (minM, maxM) = bounds.m(geometry)
      val intM = math.round(minM * pm).toInt
      writeVarInt(out, intM)
      writeVarInt(out, math.round(maxM * pm).toInt - intM)
    }

    override def writeCoordinate(out: T, coordinate: Coordinate): Unit = {
      super.writeCoordinate(out, coordinate)
      val cm = 0 // TODO math.round(coordinate.m * pm).toInt
      writeVarInt(out, cm - m)
      m = cm
    }

    override def readCoordinate(in: V): Coordinate = {
      val coord = super.readCoordinate(in)
      m = m + readVarInt(in)
      // TODO set m as 4th ordinate when supported by jts
      coord
    }

    override def skipBoundingBox(in: V): Unit = {
      super.skipBoundingBox(in)
      skipVarInt(in)
      skipVarInt(in)
    }

    override def reset(): Unit = {
      super.reset()
      m = 0
    }
  }

  private class XYZMState(precision: Int, zPrecision: Int, mPrecision: Int) extends XYZState(precision, zPrecision) {

    private val pm: Double = math.pow(10, mPrecision)
    private var m: Int = 0

    // sets bits for both z and m dims, and their precisions
    override protected val extendedDims: Byte =
      (0x03 | ((zPrecision & 0x03) << 2) | ((mPrecision & 0x03) << 5)).toByte

    override def writeBoundingBox[G <: Geometry](out: T, geometry: G)(implicit bounds: DimensionalBounds[G]): Unit = {
      super.writeBoundingBox(out, geometry)
      val (minM, maxM) = bounds.m(geometry)
      val intM = math.round(minM * pm).toInt
      writeVarInt(out, intM)
      writeVarInt(out, math.round(maxM * pm).toInt - intM)
    }

    override def writeCoordinate(out: T, coordinate: Coordinate): Unit = {
      super.writeCoordinate(out, coordinate)
      val cm = 0 // TODO math.round(coordinate.m * pm).toInt
      writeVarInt(out, cm - m)
      m = cm
    }

    override def readCoordinate(in: V): Coordinate = {
      val coord = super.readCoordinate(in)
      m = m + readVarInt(in)
      // TODO set m as 4th ordinate when supported by jts
      coord
    }

    override def skipBoundingBox(in: V): Unit = {
      super.skipBoundingBox(in)
      skipVarInt(in)
      skipVarInt(in)
    }

    override def reset(): Unit = {
      super.reset()
      m = 0
    }
  }
}

object TwkbSerialization {

  val MaxPrecision: Byte = 7

  private val ZeroByte: Byte = 0

  // twkb constants
  object GeometryBytes {
    val TwkbPoint           :Byte = 1
    val TwkbLineString      :Byte = 2
    val TwkbPolygon         :Byte = 3
    val TwkbMultiPoint      :Byte = 4
    val TwkbMultiLineString :Byte = 5
    val TwkbMultiPolygon    :Byte = 6
    val TwkbCollection      :Byte = 7
  }

  object FlagBytes {
    val BoundingBoxFlag  :Byte = 0x01
    val SizeFlag         :Byte = 0x02
    val IdsFlag          :Byte = 0x04
    val ExtendedDimsFlag :Byte = 0x08
    val EmptyFlag        :Byte = 0x10
  }
}
