/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.utils.geotools

import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS

/**
 * The object provides convenience methods for common operations on geometries.
 */
object GeometryUtils {

  val geoCalc = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
  val geoFactory = JTSFactoryFinder.getGeometryFactory

  /**
   * Returns a bounding box which circumscribes a buffered circle around a point
   *
   * @param startingPoint the Point to buffer around
   * @param distance the buffered distance in meters
   * @return A Polygon which bounds the buffered point
   */
  def bufferPoint(startPoint: Point, distance: Double): Polygon = {
    geoCalc.setStartingGeographicPoint(startPoint.getX, startPoint.getY)

    // Convert meters to dec degrees based on widest point in dec degrees of circle
    geoCalc.setDirection(90, distance)
    val right = geoCalc.getDestinationGeographicPoint
    val distanceDegrees = startPoint.distance(geoFactory.createPoint(new Coordinate(right.getX, right.getY)))

    // Walk circle bounds for bounding box
    geoCalc.setDirection(0, distance)
    val top = geoCalc.getDestinationGeographicPoint
    geoCalc.setDirection(180, distance)
    val bottom = geoCalc.getDestinationGeographicPoint
    geoCalc.setStartingGeographicPoint(top)
    geoCalc.setDirection(90, distance)
    val topRight = geoCalc.getDestinationGeographicPoint
    geoCalc.setDirection(-90, distance)
    val topLeft = geoCalc.getDestinationGeographicPoint
    geoCalc.setStartingGeographicPoint(bottom)
    geoCalc.setDirection(90, distance)
    val bottomRight = geoCalc.getDestinationGeographicPoint
    geoCalc.setDirection(-90, distance)
    val bottomLeft = geoCalc.getDestinationGeographicPoint

    val env = (new Envelope(startPoint.getCoordinate))
    env.expandToInclude(topRight.getX, topRight.getY)
    env.expandToInclude(topLeft.getX, topLeft.getY)
    env.expandToInclude(bottomRight.getX, bottomRight.getY)
    env.expandToInclude(bottomLeft.getX, bottomLeft.getY)
    JTS.toGeometry(env)
  }

  /** Convert meters to dec degrees based on widest point in dec degrees of circle */
  def distanceDegrees(startPoint: Point, meters: Double) = {
    startPoint.distance(farthestPoint(startPoint, meters))
  }

  /** Farthest point based on widest point in dec degrees of circle */
  def farthestPoint(startPoint: Point, meters: Double) = {
    val calc = new GeodeticCalculator()
    calc.setStartingGeographicPoint(startPoint.getX, startPoint.getY)
    calc.setDirection(90, meters)
    val dest2D = calc.getDestinationGeographicPoint
    geoFactory.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
  }

  // Hunter's early anti meridian stuff

  // default precision model
  val maxRealisticGeoHashPrecision : Int = 45
  val numDistinctGridPoints: Long = 1L << ((maxRealisticGeoHashPrecision+1)/2).toLong
  val defaultPrecisionModel = new PrecisionModel(numDistinctGridPoints.toDouble)

  // default factory for WGS84
  val defaultGeometryFactory : GeometryFactory = new GeometryFactory(defaultPrecisionModel, 4326)

  lazy val wholeEarthBBox = defaultGeometryFactory.createPolygon(List[Coordinate] (
    new Coordinate(-180, 90),
    new Coordinate(-180, -90),
    new Coordinate(180, -90),
    new Coordinate(180, 90),
    new Coordinate(-180, 90)).toArray)

  /**
   * Transforms a geometry with lon in (-inf, inf) and lat in [-180,180] to a geometry in whole earth BBOX.
   * Geometries with lon < -180 or lon > 180 wrap around.
   * Parts of geometries with lat outside [-90,90] are ignored.
   *
   * How it works: Translates geometry east until minimum lon [-180, 180]
   * (so that when you difference with whole earth BBOX you are guaranteed to not have any part west of whole earth)
   * Recursively translate left 360 and union with intersection of itself and wholeEarthBBox until no part left outside
   */
  def getAntimeridianSafeGeometry(targetGeom: Geometry): Geometry = {

    def recurseRight(geometryThatMayExceed180Lon: Geometry): Geometry = {
      val wholeEarthPart = wholeEarthBBox.intersection(geometryThatMayExceed180Lon)
      val outsidePart = geometryThatMayExceed180Lon.difference(wholeEarthBBox)
      (outsidePart.isEmpty || outsidePart.getEnvelopeInternal.getMaxX < 180) match {
        case (true) => wholeEarthPart
        case (false) => wholeEarthPart
          .union(recurseRight(transformGeometry(outsidePart, translateCoord(-360))))
      }
    }

    def translateCoord(degreesLonTranslation: Int): Coordinate => Coordinate =
      (coord: Coordinate) => new Coordinate(coord.x + degreesLonTranslation, coord.y)

    def transformPolygon(geometry: Geometry, transform: Coordinate => Coordinate): Geometry = {
      defaultGeometryFactory.createPolygon(geometry.getCoordinates.map(c => transform(c)))
    }

    def transformLineString(geometry: Geometry, transform: Coordinate => Coordinate): Geometry = {
      defaultGeometryFactory.createLineString(geometry.getCoordinates.map(c => transform(c)))
    }

    def transformMultiLineString(geometry: Geometry, transform: Coordinate => Coordinate): Geometry = {
      val coords = (0 until geometry.getNumGeometries).map { i => geometry.getGeometryN(i) }
      val translated = coords.map { c => transformLineString(c, transform).asInstanceOf[LineString] }
      defaultGeometryFactory.createMultiLineString(translated.toArray)
    }

    def transformMultiPolygon(geometry: Geometry, transform: Coordinate => Coordinate): Geometry = {
      val coords = (0 until geometry.getNumGeometries).map { i => geometry.getGeometryN(i) }
      val translated = coords.map { c => transformPolygon(c, transform).asInstanceOf[Polygon] }
      defaultGeometryFactory.createMultiPolygon(translated.toArray)
    }

    def transformMultiPoint(geometry: Geometry, transform: Coordinate => Coordinate): Geometry = {
      defaultGeometryFactory.createMultiPoint(geometry.getCoordinates.map(c => transform(c)))
    }

    def transformPoint(geometry: Geometry, transform: Coordinate => Coordinate): Geometry = {
      defaultGeometryFactory.createPoint(transform(geometry.getCoordinate))
    }

    def transformGeometry(geometry: Geometry, transform: Coordinate => Coordinate): Geometry = {
      geometry match {
        case p: Polygon => transformPolygon(geometry, transform)
        case l: LineString => transformLineString(geometry, transform)
        case m: MultiLineString => transformMultiLineString(geometry, transform)
        case m: MultiPolygon => transformMultiPolygon(geometry, transform)
        case m: MultiPoint => transformMultiPoint(geometry, transform)
        case p: Point => transformPoint(geometry, transform)
      }
    }

    val degreesLonTranslation = (((targetGeom.getEnvelopeInternal.getMinX + 180) / 360.0).floor * -360).toInt
    recurseRight(transformGeometry(targetGeom, translateCoord(degreesLonTranslation)))
  }

}
