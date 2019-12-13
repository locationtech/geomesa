/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import org.locationtech.jts.geom._

/**
  * Extracts the bounds from a geometry. The generic geometry envelope only deals with X and Y, this
  * also supports Z and M
  *
  * Operations expect a non-empty geometry
  *
  * @tparam T geometry type
  */
trait DimensionalBounds[T <: Geometry] {

  /**
    * Get bounds for the x dimension
    *
    * @param geometry geometry, not null and not empty
    * @return (min, max)
    */
  def x(geometry: T): (Double, Double) = (geometry.getEnvelopeInternal.getMinX, geometry.getEnvelopeInternal.getMaxX)

  /**
    * Get bounds for the y dimension
    *
    * @param geometry geometry, not null and not empty
    * @return (min, max)
    */
  def y(geometry: T): (Double, Double) = (geometry.getEnvelopeInternal.getMinY, geometry.getEnvelopeInternal.getMaxY)

  /**
    * Get bounds for the z dimension
    *
    * @param geometry geometry, not null and not empty
    * @return (min, max)
    */
  def z(geometry: T): (Double, Double)

  /**
    * Get bounds for the m dimension
    *
    * @param geometry geometry, not null and not empty
    * @return (min, max)
    */
  def m(geometry: T): (Double, Double) =
    // TODO implement once JTS supports M
    throw new NotImplementedError("JTS doesn't support M dimension")
}

object DimensionalBounds {

  implicit object LineStringBounds extends DimensionalBounds[LineString] {
    override def z(geometry: LineString): (Double, Double) = {
      var min = geometry.getCoordinateN(0).z
      var max = min
      var i = 1
      while (i < geometry.getNumPoints) {
        val z = geometry.getCoordinateN(i).z
        if (z < min) {
          min = z
        } else if (z > max) {
          max = z
        }
        i += 1
      }
      (min, max)
    }
  }

  implicit object PolygonBounds extends DimensionalBounds[Polygon] {
    override def z(geometry: Polygon): (Double, Double) = {
      var ring = geometry.getExteriorRing
      var min = ring.getCoordinateN(0).z
      var max = min
      var i = 1
      while (i < ring.getNumPoints) {
        val z = ring.getCoordinateN(i).z
        if (z < min) {
          min = z
        } else if (z > max) {
          max = z
        }
        i += 1
      }
      val numRings = geometry.getNumInteriorRing
      var j = 0
      while (j < numRings) {
        ring = geometry.getInteriorRingN(j)
        i = 0
        while (i < ring.getNumPoints) {
          val z = ring.getCoordinateN(i).z
          if (z < min) {
            min = z
          } else if (z > max) {
            max = z
          }
          i += 1
        }
        j += 1
      }
      (min, max)
    }
  }

  implicit object MultiPointBounds extends DimensionalBounds[MultiPoint] {
    override def z(geometry: MultiPoint): (Double, Double) = {
      var min = geometry.getGeometryN(0).asInstanceOf[Point].getCoordinate.z
      var max = min
      var i = 1
      while (i < geometry.getNumGeometries) {
        val z = geometry.getGeometryN(i).asInstanceOf[Point].getCoordinate.z
        if (z < min) {
          min = z
        } else if (z > max) {
          max = z
        }
        i += 1
      }
      (min, max)
    }
  }

  implicit object MultiLineStringBounds extends DimensionalBounds[MultiLineString] {
    override def z(geometry: MultiLineString): (Double, Double) = {
      var (min, max) = LineStringBounds.z(geometry.getGeometryN(0).asInstanceOf[LineString])
      var i = 1
      while (i < geometry.getNumGeometries) {
        val (mini, maxi) = LineStringBounds.z(geometry.getGeometryN(i).asInstanceOf[LineString])
        if (mini < min) {
          min = min
        }
        if (maxi > max) {
          max = maxi
        }
        i += 1
      }
      (min, max)
    }
  }

  implicit object MultiPolygonBounds extends DimensionalBounds[MultiPolygon] {
    override def z(geometry: MultiPolygon): (Double, Double) = {
      var (min, max) = PolygonBounds.z(geometry.getGeometryN(0).asInstanceOf[Polygon])
      var i = 1
      while (i < geometry.getNumGeometries) {
        val (mini, maxi) = PolygonBounds.z(geometry.getGeometryN(i).asInstanceOf[Polygon])
        if (mini < min) {
          min = min
        }
        if (maxi > max) {
          max = maxi
        }
        i += 1
      }
      (min, max)
    }
  }

  implicit object GeometryCollectionBounds extends DimensionalBounds[GeometryCollection] {
    override def z(geometry: GeometryCollection): (Double, Double) = {
      var (min, max) = geometry.getGeometryN(0) match {
        case g: Point              => (g.getCoordinate.z, g.getCoordinate.z)
        case g: LineString         => LineStringBounds.z(g)
        case g: Polygon            => PolygonBounds.z(g)
        case g: MultiPoint         => MultiPointBounds.z(g)
        case g: MultiLineString    => MultiLineStringBounds.z(g)
        case g: MultiPolygon       => MultiPolygonBounds.z(g)
        case g: GeometryCollection => GeometryCollectionBounds.z(g)
      }
      var i = 1
      while (i < geometry.getNumGeometries) {
        val (mini, maxi) = geometry.getGeometryN(i) match {
          case g: Point              => (g.getCoordinate.z, g.getCoordinate.z)
          case g: LineString         => LineStringBounds.z(g)
          case g: Polygon            => PolygonBounds.z(g)
          case g: MultiPoint         => MultiPointBounds.z(g)
          case g: MultiLineString    => MultiLineStringBounds.z(g)
          case g: MultiPolygon       => MultiPolygonBounds.z(g)
          case g: GeometryCollection => GeometryCollectionBounds.z(g)
        }
        if (mini < min) {
          min = min
        }
        if (maxi > max) {
          max = maxi
        }
        i += 1
      }
      (min, max)
    }
  }
}
