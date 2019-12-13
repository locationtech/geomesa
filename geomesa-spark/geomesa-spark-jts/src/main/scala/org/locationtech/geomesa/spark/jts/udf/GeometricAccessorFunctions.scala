/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import java.{lang => jl}

import org.locationtech.jts.geom._
import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._

import scala.util.Try

object GeometricAccessorFunctions {
  val ST_Boundary: Geometry => Geometry = nullableUDF(geom => geom.getBoundary)
  val ST_CoordDim: Geometry => Integer = nullableUDF(geom => {
    val coord = geom.getCoordinate
    if (coord.z.isNaN) 2 else 3
  })
  val ST_Dimension: Geometry => Integer = nullableUDF(geom => geom.getDimension)
  val ST_Envelope: Geometry => Geometry = nullableUDF(geom => geom.getEnvelope)
  val ST_ExteriorRing: Geometry => LineString = {
    case geom: Polygon => geom.getExteriorRing
    case _ => null
  }
  val ST_GeometryN: (Geometry, Int) => Geometry = nullableUDF { (geom, n) =>
    if (n > 0 && n <= geom.getNumGeometries) { geom.getGeometryN(n - 1) } else { null }
  }
  val ST_GeometryType: Geometry => String = nullableUDF(geom => geom.getGeometryType)
  val ST_InteriorRingN: (Geometry, Int) => Geometry = nullableUDF { (geom, n) =>
    geom match {
      case geom: Polygon =>
        if (0 < n && n <= geom.getNumInteriorRing) {
          geom.getInteriorRingN(n - 1)
        } else {
          null
        }
      case _ => null
    }
  }
  val ST_IsClosed: Geometry => jl.Boolean = nullableUDF({
    case geom: LineString => geom.isClosed
    case geom: MultiLineString => geom.isClosed
    case _ => true
  })
  val ST_IsCollection: Geometry => jl.Boolean = nullableUDF(geom => geom.isInstanceOf[GeometryCollection])
  val ST_IsEmpty: Geometry => jl.Boolean = nullableUDF(geom => geom.isEmpty)
  val ST_IsRing: Geometry => jl.Boolean = nullableUDF({
    case geom: LineString => geom.isClosed && geom.isSimple
    case geom: MultiLineString => geom.isClosed && geom.isSimple
    case geom => geom.isSimple
  })
  val ST_IsSimple: Geometry => jl.Boolean = nullableUDF(geom => geom.isSimple)
  val ST_IsValid: Geometry => jl.Boolean = nullableUDF(geom => geom.isValid)
  val ST_NumGeometries: Geometry => Integer = nullableUDF(geom => geom.getNumGeometries)
  val ST_NumPoints: Geometry => Integer = nullableUDF(geom => geom.getNumPoints)
  val ST_PointN: (Geometry, Int) => Point = nullableUDF { (geom, n) =>
    geom match {
      case g: LineString =>
        if (n > 0 && n <= g.getNumPoints) {
          g.getPointN(n - 1)
        } else if (n < 0 && n + g.getNumPoints >= 0) {
          g.getPointN(n + g.getNumPoints)
        } else {
          null
        }
      case _ => null
    }
  }
  val ST_X: Geometry => jl.Float = {
    case geom: Point => geom.getX.toFloat
    case _ => null
  }
  val ST_Y: Geometry => jl.Float = {
    case geom: Point => geom.getY.toFloat
    case _ => null
  }

  private[geomesa] val accessorNames = Map(
    ST_Boundary -> "st_boundary",
    ST_CoordDim -> "st_coordDim",
    ST_Dimension -> "st_dimension",
    ST_Envelope -> "st_envelope",
    ST_ExteriorRing -> "st_exteriorRing",
    ST_GeometryN -> "st_geometryN",
    ST_GeometryType -> "st_geometryType",
    ST_InteriorRingN -> "st_interiorRingN",
    ST_IsClosed -> "st_isClosed",
    ST_IsCollection -> "st_isCollection",
    ST_IsEmpty -> "st_isEmpty",
    ST_IsRing -> "st_isRing",
    ST_IsSimple -> "st_isSimple",
    ST_IsValid -> "st_isValid",
    ST_NumGeometries -> "st_numGeometries",
    ST_NumPoints -> "st_numPoints",
    ST_PointN -> "st_pointN",
    ST_X -> "st_x",
    ST_Y -> "st_y"
  )

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(accessorNames(ST_Boundary), ST_Boundary)
    sqlContext.udf.register(accessorNames(ST_CoordDim), ST_CoordDim)
    sqlContext.udf.register(accessorNames(ST_Dimension), ST_Dimension)
    sqlContext.udf.register(accessorNames(ST_Envelope), ST_Envelope)
    sqlContext.udf.register(accessorNames(ST_ExteriorRing), ST_ExteriorRing)
    sqlContext.udf.register(accessorNames(ST_GeometryN), ST_GeometryN)
    sqlContext.udf.register(accessorNames(ST_GeometryType), ST_GeometryType)
    sqlContext.udf.register(accessorNames(ST_InteriorRingN), ST_InteriorRingN)
    sqlContext.udf.register(accessorNames(ST_IsClosed), ST_IsClosed)
    sqlContext.udf.register(accessorNames(ST_IsCollection), ST_IsCollection)
    sqlContext.udf.register(accessorNames(ST_IsEmpty), ST_IsEmpty)
    sqlContext.udf.register(accessorNames(ST_IsRing), ST_IsRing)
    sqlContext.udf.register(accessorNames(ST_IsSimple), ST_IsSimple)
    sqlContext.udf.register(accessorNames(ST_IsValid), ST_IsValid)
    sqlContext.udf.register(accessorNames(ST_NumGeometries), ST_NumGeometries)
    sqlContext.udf.register(accessorNames(ST_NumPoints), ST_NumPoints)
    sqlContext.udf.register(accessorNames(ST_PointN), ST_PointN)
    sqlContext.udf.register(accessorNames(ST_X), ST_X)
    sqlContext.udf.register(accessorNames(ST_Y), ST_Y)
  }
}
