/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.features.serialization

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKBConstants
import org.locationtech.geomesa.utils.text.WKBUtils

import scala.reflect.ClassTag

/** A [[DatumReader]] for [[Geometry]].
 *
 */
trait GeometryReader[Reader] extends PrimitiveReader[Reader] with NullableReader[Reader] {

  private lazy val factory = new GeometryFactory()
  private lazy val csFactory = factory.getCoordinateSequenceFactory

  /** Selects the correct [[Geometry]] reader, either ``readGeometryDirectly`` or ``readGeometryAsWKB``
    * depending on the serialization ``version``.
    */
  def selectGeometryReader(version: Version): DatumReader[Reader, Geometry]

  /** Selects the correct reader based on the type of geometry.  For use only when reading [[Geometry]] directly. */
  val selectGeometryReader: DatumReader[Reader, Geometry] = (in) => {
    readPositiveInt(in) match {
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

  def readPolygon(in: Reader): Polygon = {
    val exteriorRing = factory.createLinearRing(readCoordinateSequence(in))
    val numInteriorRings = readPositiveInt(in)
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

  def readGeometryCollection[T <: Geometry: ClassTag](in: Reader): Array[T] = {
    val numGeoms = readPositiveInt(in)
    val geoms = Array.ofDim[T](numGeoms)
    var i = 0
    while (i < numGeoms) {
      geoms.update(i, readGeometryDirectly(in).asInstanceOf[T])
      i += 1
    }
    geoms
  }

  def readCoordinateSequence(in: Reader): CoordinateSequence = {
    val numCoords = readPositiveInt(in)
    val coords = csFactory.create(numCoords, 2)
    var i = 0
    while (i < numCoords) {
      coords.setOrdinate(i, 0, readDouble(in))
      coords.setOrdinate(i, 1, readDouble(in))
      i += 1
    }
    coords
  }

  def readCoordinate(in: Reader): CoordinateSequence = {
    val coords = csFactory.create(1, 2)
    coords.setOrdinate(0, 0, readDouble(in))
    coords.setOrdinate(0, 1, readDouble(in))
    coords
  }

  /** Based on the method from geotools WKBReader. */
  val readGeometryDirectly: DatumReader[Reader, Geometry] = readNullable(selectGeometryReader)

  lazy val readGeometryAsWKB: DatumReader[Reader, Geometry] = (reader) => {
    val bytes = readBytes(reader)
    if (bytes.length > 0) {
      WKBUtils.read(bytes)
    } else {
      null
    }
  }
}
