/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.locationtech.jts.geom._
import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.operation.distance.DistanceOp
import org.apache.spark.sql.SQLContext
import org.locationtech.spatial4j.distance.{DistanceCalculator, DistanceUtils}
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.geomesa.spark.jts.udaf.ConvexHull
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._

object SpatialRelationFunctions {
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
  val ST_RelateBool: (Geometry, Geometry, String) => jl.Boolean = nullableUDF((geom1, geom2, pattern) => geom1.relate(geom2, pattern))

  val ST_Area: Geometry => jl.Double = nullableUDF(g => g.getArea)
  val ST_Centroid: Geometry => Point = nullableUDF(g => g.getCentroid)
  val ST_ClosestPoint: (Geometry, Geometry) => Point =
    nullableUDF((g1, g2) => closestPoint(g1, g2))
  val ST_Distance: (Geometry, Geometry) => jl.Double =
    nullableUDF((g1, g2) => g1.distance(g2))
  val ST_DistanceSphere: (Geometry, Geometry) => jl.Double =
    nullableUDF((s, e) => fastDistance(s.getCoordinate, e.getCoordinate))
  val ST_Length: Geometry => jl.Double = nullableUDF(g => g.getLength)

  // Assumes input is two points, for use with collect_list and window functions
  val ST_AggregateDistanceSphere: Seq[Geometry] => jl.Double = a => ST_DistanceSphere(a(0), a(1))

  val ST_LengthSphere: LineString => jl.Double =
    nullableUDF(line => line.getCoordinates.sliding(2).map { case Array(l, r) => fastDistance(l, r) }.sum)

  private[geomesa] val relationNames = Map(
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
    ST_DistanceSphere -> "st_distanceSphere",
    ST_Length -> "st_length",
    ST_AggregateDistanceSphere -> "st_aggregateDistanceSphere",
    ST_LengthSphere -> "st_lengthSphere"
  )

  // Geometry Processing
  private[geomesa] val ch = new ConvexHull

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register geometry editors
    sqlContext.udf.register(relationNames(ST_Translate), ST_Translate)

    // Register spatial relationships
    sqlContext.udf.register(relationNames(ST_Contains), ST_Contains)
    sqlContext.udf.register(relationNames(ST_Covers), ST_Covers)
    sqlContext.udf.register(relationNames(ST_Crosses), ST_Crosses)
    sqlContext.udf.register(relationNames(ST_Disjoint), ST_Disjoint)
    sqlContext.udf.register(relationNames(ST_Equals), ST_Equals)
    sqlContext.udf.register(relationNames(ST_Intersects), ST_Intersects)
    sqlContext.udf.register(relationNames(ST_Overlaps), ST_Overlaps)
    sqlContext.udf.register(relationNames(ST_Touches), ST_Touches)
    sqlContext.udf.register(relationNames(ST_Within), ST_Within)
    // renamed st_relate variant that returns a boolean since
    // Spark SQL doesn't seem to support polymorphic UDFs
    sqlContext.udf.register(relationNames(ST_Relate), ST_Relate)
    sqlContext.udf.register(relationNames(ST_RelateBool), ST_RelateBool)

    sqlContext.udf.register(relationNames(ST_Area), ST_Area)
    sqlContext.udf.register(relationNames(ST_ClosestPoint), ST_ClosestPoint)
    sqlContext.udf.register(relationNames(ST_Centroid), ST_Centroid)
    sqlContext.udf.register(relationNames(ST_Distance), ST_Distance)
    sqlContext.udf.register(relationNames(ST_Length), ST_Length)

    sqlContext.udf.register(relationNames(ST_DistanceSphere), ST_DistanceSphere)
    sqlContext.udf.register(relationNames(ST_AggregateDistanceSphere), ST_AggregateDistanceSphere)
    sqlContext.udf.register(relationNames(ST_LengthSphere), ST_LengthSphere)

    // Register geometry Processing
    sqlContext.udf.register("st_convexhull", ch)
  }

  @transient private lazy val spatialContext = JtsSpatialContext.GEO

  @transient private val geoCalcs = new ThreadLocal[DistanceCalculator] {
    override def initialValue(): DistanceCalculator = spatialContext.getDistCalc
  }
  @transient private[geomesa] val geomFactory = new GeometryFactory()

  def closestPoint(g1: Geometry, g2: Geometry): Point = {
    val op = new DistanceOp(g1, g2)
    val coord = op.nearestPoints()
    geomFactory.createPoint(coord(0))
  }

  def fastDistance(c1: Coordinate, c2: Coordinate): Double = {
    val calc = geoCalcs.get()
    val startPoint = spatialContext.getShapeFactory.pointXY(c1.x, c1.y)
    DistanceUtils.DEG_TO_KM * calc.distance(startPoint, c2.x, c2.y) * 1000
  }

  def translate(g: Geometry, deltax: Double, deltay: Double): Geometry = {
    val affineTransform = new AffineTransformation()
    affineTransform.setToTranslation(deltax, deltay)
    affineTransform.transform(g)
  }
}
