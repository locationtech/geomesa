/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import org.locationtech.geomesa.features.serialization.DimensionalBounds.{CoordAccessor, MAccessor, ZAccessor}
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
  def z(geometry: T): (Double, Double) = bounds(geometry, ZAccessor)

  /**
    * Get bounds for the m dimension
    *
    * @param geometry geometry, not null and not empty
    * @return (min, max)
    */
  def m(geometry: T): (Double, Double) = bounds(geometry, MAccessor)

  private [serialization] def bounds(geometry: T, accessor: CoordAccessor): (Double, Double)
}

object DimensionalBounds {

  implicit object LineStringBounds extends DimensionalBounds[LineString] {
    override private [serialization] def bounds(
        geometry: LineString,
        accessor: CoordAccessor): (Double, Double) = {
      var min = accessor(geometry.getCoordinateN(0))
      var max = min
      var i = 1
      while (i < geometry.getNumPoints) {
        val next = accessor(geometry.getCoordinateN(i))
        if (next < min) {
          min = next
        } else if (next > max) {
          max = next
        }
        i += 1
      }
      (min, max)
    }
  }

  implicit object PolygonBounds extends DimensionalBounds[Polygon] {
    override private [serialization] def bounds(
        geometry: Polygon,
        accessor: CoordAccessor): (Double, Double) = {
      var ring = geometry.getExteriorRing
      var min = accessor(ring.getCoordinateN(0))
      var max = min
      var i = 1
      while (i < ring.getNumPoints) {
        val next = accessor(ring.getCoordinateN(i))
        if (next < min) {
          min = next
        } else if (next > max) {
          max = next
        }
        i += 1
      }
      val numRings = geometry.getNumInteriorRing
      var j = 0
      while (j < numRings) {
        ring = geometry.getInteriorRingN(j)
        i = 0
        while (i < ring.getNumPoints) {
          val next = accessor(ring.getCoordinateN(i))
          if (next < min) {
            min = next
          } else if (next > max) {
            max = next
          }
          i += 1
        }
        j += 1
      }
      (min, max)
    }
  }

  implicit object MultiPointBounds extends DimensionalBounds[MultiPoint] {
    override private [serialization] def bounds(
        geometry: MultiPoint,
        accessor: CoordAccessor): (Double, Double) = {
      var min = accessor(geometry.getGeometryN(0).asInstanceOf[Point].getCoordinate)
      var max = min
      var i = 1
      while (i < geometry.getNumGeometries) {
        val next = accessor(geometry.getGeometryN(i).asInstanceOf[Point].getCoordinate)
        if (next < min) {
          min = next
        } else if (next > max) {
          max = next
        }
        i += 1
      }
      (min, max)
    }
  }

  implicit object MultiLineStringBounds extends DimensionalBounds[MultiLineString] {
    override private [serialization] def bounds(
        geometry: MultiLineString,
        accessor: CoordAccessor): (Double, Double) = {
      var (min, max) = LineStringBounds.bounds(geometry.getGeometryN(0).asInstanceOf[LineString], accessor)
      var i = 1
      while (i < geometry.getNumGeometries) {
        val (mini, maxi) = LineStringBounds.bounds(geometry.getGeometryN(i).asInstanceOf[LineString], accessor)
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
    override private [serialization] def bounds(
        geometry: MultiPolygon,
        accessor: CoordAccessor): (Double, Double) = {
      var (min, max) = PolygonBounds.bounds(geometry.getGeometryN(0).asInstanceOf[Polygon], accessor)
      var i = 1
      while (i < geometry.getNumGeometries) {
        val (mini, maxi) = PolygonBounds.bounds(geometry.getGeometryN(i).asInstanceOf[Polygon], accessor)
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
    override private [serialization] def bounds(
        geometry: GeometryCollection,
        accessor: CoordAccessor): (Double, Double) = {
      var (min, max) = geometry.getGeometryN(0) match {
        case g: Point              => (accessor(g.getCoordinate), accessor(g.getCoordinate))
        case g: LineString         => LineStringBounds.bounds(g, accessor)
        case g: Polygon            => PolygonBounds.bounds(g, accessor)
        case g: MultiPoint         => MultiPointBounds.bounds(g, accessor)
        case g: MultiLineString    => MultiLineStringBounds.bounds(g, accessor)
        case g: MultiPolygon       => MultiPolygonBounds.bounds(g, accessor)
        case g: GeometryCollection => GeometryCollectionBounds.bounds(g, accessor)
      }
      var i = 1
      while (i < geometry.getNumGeometries) {
        val (mini, maxi) = geometry.getGeometryN(i) match {
          case g: Point              => (accessor(g.getCoordinate), accessor(g.getCoordinate))
          case g: LineString         => LineStringBounds.bounds(g, accessor)
          case g: Polygon            => PolygonBounds.bounds(g, accessor)
          case g: MultiPoint         => MultiPointBounds.bounds(g, accessor)
          case g: MultiLineString    => MultiLineStringBounds.bounds(g, accessor)
          case g: MultiPolygon       => MultiPolygonBounds.bounds(g, accessor)
          case g: GeometryCollection => GeometryCollectionBounds.bounds(g, accessor)
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

  private trait CoordAccessor {
    def apply(c: Coordinate): Double
  }

  private object ZAccessor extends CoordAccessor {
    override def apply(c: Coordinate): Double = c.getZ
  }

  private object MAccessor extends CoordAccessor {
    override def apply(c: Coordinate): Double = c.getM
  }
}
