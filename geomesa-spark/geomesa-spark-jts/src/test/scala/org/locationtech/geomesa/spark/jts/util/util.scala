package org.locationtech.geomesa.spark.jts.util

import com.vividsolutions.jts.geom.{Geometry, LineString, Point, Polygon}

package object util {
  case class PointContainer(geom: Point)
  case class PolygonContainer(geom: Polygon)
  case class LineStringContainer(geom: LineString)
  case class GeometryContainer(geom: Geometry)
}
