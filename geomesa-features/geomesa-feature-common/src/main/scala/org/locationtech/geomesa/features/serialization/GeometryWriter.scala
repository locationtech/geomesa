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

/** A [[DatumWriter]] for [[Geometry]].
  *
  */
trait GeometryWriter[Writer] extends PrimitiveWriter[Writer] with NullableWriter[Writer] {

  val selectGeometryWriter: DatumWriter[Writer, Geometry] = (out, geom) =>
    geom match {
      case g: Point =>
        writePositiveInt(out, WKBConstants.wkbPoint)
        writeCoordinate(out, g.getCoordinateSequence.getCoordinate(0))

      case g: LineString =>
        writePositiveInt(out, WKBConstants.wkbLineString)
        writeCoordinateSequence(out, g.getCoordinateSequence)

      case g: Polygon => writePolygon(out, g)

      case g: MultiPoint => writeGeometryCollection(out, WKBConstants.wkbMultiPoint, g)

      case g: MultiLineString => writeGeometryCollection(out, WKBConstants.wkbMultiLineString, g)

      case g: MultiPolygon => writeGeometryCollection(out, WKBConstants.wkbMultiPolygon, g)

      case g: GeometryCollection => writeGeometryCollection(out, WKBConstants.wkbGeometryCollection, g)
    }

  def writePolygon(out: Writer, g: Polygon): Unit = {
    writePositiveInt(out, WKBConstants.wkbPolygon)
    writeCoordinateSequence(out, g.getExteriorRing.getCoordinateSequence)
    writePositiveInt(out, g.getNumInteriorRing)
    var i = 0
    while (i < g.getNumInteriorRing) {
      writeCoordinateSequence(out, g.getInteriorRingN(i).getCoordinateSequence)
      i += 1
    }
  }

  def writeGeometryCollection(out: Writer, typ: Int, g: GeometryCollection): Unit = {
    writePositiveInt(out, typ)
    writePositiveInt(out, g.getNumGeometries)
    var i = 0
    while (i < g.getNumGeometries) {
      writeGeometry(out, g.getGeometryN(i))
      i += 1
    }
  }

  def writeCoordinateSequence(out: Writer, coords: CoordinateSequence): Unit = {
    writePositiveInt(out, coords.size())
    var i = 0
    while (i < coords.size()) {
      writeCoordinate(out, coords.getCoordinate(i))
      i += 1
    }
  }

  def writeCoordinate(out: Writer, coord: Coordinate): Unit = {
    writeDouble(out, coord.getOrdinate(0))
    writeDouble(out, coord.getOrdinate(1))
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
  val writeGeometry: DatumWriter[Writer, Geometry] = writeNullable(selectGeometryWriter)
}
