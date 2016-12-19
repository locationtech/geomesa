/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.serialization

import com.vividsolutions.jts.geom._
import org.locationtech.geomesa.utils.text.WKBUtils

import scala.reflect.ClassTag

/** A [[DatumReader]] for [[Geometry]].
 *
 */
trait GeometryReader[Reader] extends PrimitiveReader[Reader] with NullableReader[Reader] {

  // note: dimensions have to be determined from the internal coordinate sequence, not the geometry itself.

  import GeometrySerialization._

  private lazy val factory = new GeometryFactory()
  private lazy val csFactory = factory.getCoordinateSequenceFactory

  /** Selects the correct [[Geometry]] reader, either ``readGeometryDirectly`` or ``readGeometryAsWKB``
    * depending on the serialization ``version``.
    */
  def selectGeometryReader(version: Version): DatumReader[Reader, Geometry]

  /** Selects the correct reader based on the type of geometry.  For use only when reading [[Geometry]] directly. */
  def readGeometry(in: Reader): Geometry = {
    readPositiveInt(in) match {
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
    }
  }

  private def readPoint(in: Reader, dims: Option[Int]): Point =
    factory.createPoint(readCoordinateSequence(in, Some(1), dims))

  private def readLineString(in: Reader, dims: Option[Int]): LineString =
    factory.createLineString(readCoordinateSequence(in, None, dims))

  private def readPolygon(in: Reader, dims: Option[Int]): Polygon = {
    val exteriorRing = factory.createLinearRing(readCoordinateSequence(in, None, dims))
    val numInteriorRings = readPositiveInt(in)
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

  private def readGeometryCollection[U <: Geometry: ClassTag](in: Reader): Array[U] = {
    val numGeoms = readPositiveInt(in)
    val geoms = Array.ofDim[U](numGeoms)
    var i = 0
    while (i < numGeoms) {
      geoms.update(i, readGeometryDirectly(in).asInstanceOf[U])
      i += 1
    }
    geoms
  }

  private def readCoordinateSequence(in: Reader, length: Option[Int], dimensions: Option[Int]): CoordinateSequence = {
    val numCoords = length.getOrElse(readPositiveInt(in))
    val numDims = dimensions.getOrElse(readPositiveInt(in))
    val coords = csFactory.create(numCoords, numDims)
    var i = 0
    while (i < numCoords) {
      var j = 0
      while (j < numDims) {
        coords.setOrdinate(i, j, readDouble(in))
        j += 1
      }
      i += 1
    }
    coords
  }

  /** Based on the method from geotools WKBReader. */
  val readGeometryDirectly: DatumReader[Reader, Geometry] = readNullable((in) => readGeometry(in))

  lazy val readGeometryAsWKB: DatumReader[Reader, Geometry] = (reader) => {
    val bytes = readBytes(reader)
    if (bytes.length > 0) {
      WKBUtils.read(bytes)
    } else {
      null
    }
  }
}
