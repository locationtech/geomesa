/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.awt.geom.AffineTransform

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.operation.distance.DistanceOp
import org.apache.spark.sql.{Column, SQLContext, TypedColumn}
import org.apache.spark.sql.udaf.ConvexHull
import org.locationtech.geomesa.spark.SQLFunctionHelper._
import org.locationtech.geomesa.spark.SparkDefaultEncoders._
import org.locationtech.geomesa.spark.SpatialEncoders._
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.referencing.operation.transform.AffineTransform2D

object SQLSpatialFunctions {
  import java.{lang => jl}

  // Geometry editors
  val ST_Translate: (Geometry, Double, Double) => Geometry =
    (g, deltaX, deltaY) => translate(g, deltaX, deltaY)

  // Spatial relationships
  // DE-9IM relations
  val ST_Contains:   (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.contains(geom2))
  val ST_Covers:     (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.covers(geom2))
  val ST_Crosses:    (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.crosses(geom2))
  val ST_Disjoint:   (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.disjoint(geom2))
  val ST_Equals:     (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.equals(geom2))
  val ST_Intersects: (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.intersects(geom2))
  val ST_Overlaps:   (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.overlaps(geom2))
  val ST_Touches:    (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.touches(geom2))
  val ST_Within:     (Geometry, Geometry) => jl.Boolean = nullableUDF((geom1, geom2) => geom1.within(geom2))
  val ST_Relate:     (Geometry, Geometry) => String = nullableUDF((geom1, geom2) => geom1.relate(geom2).toString)
  val ST_RelateBool: (Geometry, Geometry, String) => Boolean =
    nullableUDF((geom1, geom2, pattern) => geom1.relate(geom2, pattern))

  val ST_Area: Geometry => jl.Double = nullableUDF(g => g.getArea)
  val ST_Centroid: Geometry => Point = nullableUDF(g => g.getCentroid)
  val ST_ClosestPoint: (Geometry, Geometry) => Point =
    nullableUDF((g1, g2) => closestPoint(g1, g2))
  val ST_Distance: (Geometry, Geometry) => jl.Double =
    nullableUDF((g1, g2) => g1.distance(g2))
  val ST_DistanceSpheroid: (Geometry, Geometry) => jl.Double =
    nullableUDF((s, e) => fastDistance(s.getCoordinate, e.getCoordinate))
  val ST_Length: Geometry => jl.Double = nullableUDF(g => g.getLength)

  // Assumes input is two points, for use with collect_list and window functions
  val ST_AggregateDistanceSpheroid: Seq[Geometry] => jl.Double = a => ST_DistanceSpheroid(a(0), a(1))

  val ST_LengthSpheroid: LineString => jl.Double =
    nullableUDF(line => line.getCoordinates.sliding(2).map { case Array(l, r) => fastDistance(l, r) }.sum)


  private[geomesa] val namer = Map(
    ST_Translate -> "st_translate" ,
    ST_Contains -> "st_contains",
    ST_Covers -> "st_covers",
    ST_Crosses -> "st_crosses",
    ST_Disjoint -> "st_disjoint",
    ST_Equals -> "st_equals",
    ST_Intersects -> "st_intersects",
    ST_Overlaps -> "st_overlaps",
    ST_Touches -> "st_touches",
    ST_Within -> "st_within",
    ST_Relate -> "st_relate",
    ST_RelateBool -> "st_relateBool",
    ST_Area -> "st_area",
    ST_Centroid -> "st_centroid",
    ST_ClosestPoint -> "st_closestPoint",
    ST_Distance -> "st_distance",
    ST_DistanceSpheroid -> "st_distanceSpheroid",
    ST_Length -> "st_length",
    ST_AggregateDistanceSpheroid -> "st_aggregateDistanceSpheroid",
    ST_LengthSpheroid -> "st_lengthSpheroid"
  )

  // DataFrame DSL function wrappers

  def st_translate(geom: Column, deltaX: Column, deltaY: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_Translate, namer, geom, deltaX, deltaY)
  def st_translate(geom: Geometry, deltaX: Double, deltaY: Double): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_Translate, namer, geom, deltaX, deltaY)

  def st_contains(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Contains, namer, left, right)
  def st_contains(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Contains, namer, left, right)

  def st_covers(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Covers, namer, left, right)
  def st_covers(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Covers, namer, left, right)

  def st_crosses(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Crosses, namer, left, right)
  def st_crosses(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Crosses, namer, left, right)

  def st_disjoint(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Disjoint, namer, left, right)
  def st_disjoint(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Disjoint, namer, left, right)

  def st_equals(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Equals, namer, left, right)
  def st_equals(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Equals, namer, left, right)

  def st_intersects(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Intersects, namer, left, right)
  def st_intersects(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Intersects, namer, left, right)

  def st_overlaps(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Overlaps, namer, left, right)
  def st_overlaps(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Overlaps, namer, left, right)

  def st_touches(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Touches, namer, left, right)
  def st_touches(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Touches, namer, left, right)

  def st_within(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_Within, namer, left, right)
  def st_within(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_Within, namer, left, right)

  def st_relate(left: Column, right: Column): TypedColumn[Any, String] =
    udfToColumn(ST_Relate, namer, left, right)
  def st_relate(left: Geometry, right: Geometry): TypedColumn[Any, String] =
    udfToColumnLiterals(ST_Relate, namer, left, right)

  def st_relateBool(left: Column, right: Column, pattern: Column): TypedColumn[Any, Boolean] =
    udfToColumn(ST_RelateBool, namer, left, right, pattern)
  def st_relateBool(left: Geometry, right: Geometry, pattern: String): TypedColumn[Any, Boolean] =
    udfToColumnLiterals(ST_RelateBool, namer, left, right, pattern)

  def st_area(geom: Column): TypedColumn[Any, jl.Double] =
    udfToColumn(ST_Area, namer, geom)
  def st_area(geom: Geometry): TypedColumn[Any, jl.Double] =
    udfToColumnLiterals(ST_Area, namer, geom)

  def st_closestPoint(left: Column, right: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_ClosestPoint, namer, left, right)
  def st_closestPoint(left: Geometry, right: Geometry): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_ClosestPoint, namer, left, right)

  def st_centroid(geom: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_Centroid, namer, geom)
  def st_centroid(geom: Geometry): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_Centroid, namer, geom)

  def st_distance(left: Column, right: Column): TypedColumn[Any, jl.Double] =
    udfToColumn(ST_Distance, namer, left, right)
  def st_distance(left: Geometry, right: Geometry): TypedColumn[Any, jl.Double] =
    udfToColumnLiterals(ST_Distance, namer, left, right)

  def st_distanceSpheroid(left: Column, right: Column): TypedColumn[Any, jl.Double] =
    udfToColumn(ST_DistanceSpheroid, namer, left, right)
  def st_distanceSpheroid(left: Geometry, right: Geometry): TypedColumn[Any, jl.Double] =
    udfToColumnLiterals(ST_DistanceSpheroid, namer, left, right)

  def st_length(geom: Column): TypedColumn[Any, jl.Double] = udfToColumn(ST_Length, namer, geom)
  def st_length(geom: Geometry): TypedColumn[Any, jl.Double] = udfToColumnLiterals(ST_Length, namer, geom)

  def st_aggregateDistanceSpheroid(geomSeq: Column): TypedColumn[Any, jl.Double] =
    udfToColumn(ST_AggregateDistanceSpheroid, namer, geomSeq)
  def st_aggregateDistanceSpheroid(geomSeq: Seq[Geometry]): TypedColumn[Any, jl.Double] =
    udfToColumnLiterals(ST_AggregateDistanceSpheroid, namer, geomSeq)

  def st_lengthSpheroid(line: Column): TypedColumn[Any, jl.Double] =
    udfToColumn(ST_LengthSpheroid, namer, line)
  def st_lengthSpheroid(line: LineString): TypedColumn[Any, jl.Double] =
    udfToColumnLiterals(ST_LengthSpheroid, namer, line)



  // Geometry Processing
  private[geomesa] val ch = new ConvexHull

  def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register geometry editors
    sqlContext.udf.register(namer(ST_Translate), ST_Translate)

    // Register spatial relationships
    sqlContext.udf.register(namer(ST_Contains), ST_Contains)
    sqlContext.udf.register(namer(ST_Covers), ST_Covers)
    sqlContext.udf.register(namer(ST_Crosses), ST_Crosses)
    sqlContext.udf.register(namer(ST_Disjoint), ST_Disjoint)
    sqlContext.udf.register(namer(ST_Equals), ST_Equals)
    sqlContext.udf.register(namer(ST_Intersects), ST_Intersects)
    sqlContext.udf.register(namer(ST_Overlaps), ST_Overlaps)
    sqlContext.udf.register(namer(ST_Touches), ST_Touches)
    sqlContext.udf.register(namer(ST_Within), ST_Within)
    // renamed st_relate variant that returns a boolean since
    // Spark SQL doesn't seem to support polymorphic UDFs
    sqlContext.udf.register(namer(ST_Relate), ST_Relate)
    sqlContext.udf.register(namer(ST_RelateBool), ST_RelateBool)

    sqlContext.udf.register(namer(ST_Area), ST_Area)
    sqlContext.udf.register(namer(ST_ClosestPoint), ST_ClosestPoint)
    sqlContext.udf.register(namer(ST_Centroid), ST_Centroid)
    sqlContext.udf.register(namer(ST_Distance), ST_Distance)
    sqlContext.udf.register(namer(ST_Length), ST_Length)

    sqlContext.udf.register(namer(ST_DistanceSpheroid), ST_DistanceSpheroid)
    sqlContext.udf.register(namer(ST_AggregateDistanceSpheroid), ST_AggregateDistanceSpheroid)
    sqlContext.udf.register(namer(ST_LengthSpheroid), ST_LengthSpheroid)

    // Register geometry Processing
    sqlContext.udf.register("st_convexhull", ch)
  }

  @transient private val geoCalcs = new ThreadLocal[GeodeticCalculator] {
    override def initialValue(): GeodeticCalculator = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
  }
  @transient private[geomesa] val geomFactory = JTSFactoryFinder.getGeometryFactory

  def closestPoint(g1: Geometry, g2: Geometry): Point = {
    val op = new DistanceOp(g1, g2)
    val coord = op.nearestPoints()
    geomFactory.createPoint(coord(0))
  }

  def fastDistance(c1: Coordinate, c2: Coordinate): Double = {
    val calc = geoCalcs.get()
    calc.setStartingGeographicPoint(c1.x, c1.y)
    calc.setDestinationGeographicPoint(c2.x, c2.y)
    calc.getOrthodromicDistance
  }

  def translate(g: Geometry, deltax: Double, deltay: Double): Geometry = {
    val affineTransform = AffineTransform.getTranslateInstance(deltax, deltay)
    val transform = new AffineTransform2D(affineTransform)
    JTS.transform(g, transform)
  }
}
