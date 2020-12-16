/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.udaf.ConvexHull
import org.locationtech.geomesa.spark.jts.udf.NullableUDF._
import org.locationtech.geomesa.spark.jts.udf.UDFFactory.Registerable
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.operation.distance.DistanceOp
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.distance.{DistanceCalculator, DistanceUtils}

object SpatialRelationFunctions extends UDFFactory {

  // Geometry editors
  class ST_Translate extends NullableUDF3[Geometry, Double, Double, Geometry](translate)
  val ST_Translate = new ST_Translate()

  // Spatial relationships
  // DE-9IM relations
  class ST_Contains   extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ contains _)
  class ST_Covers     extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ covers _)
  class ST_Crosses    extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ crosses _)
  class ST_Disjoint   extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ disjoint _)
  class ST_Equals     extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ equals _)
  class ST_Intersects extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ intersects _)
  class ST_Overlaps   extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ overlaps _)
  class ST_Touches    extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ touches _)
  class ST_Within     extends NullableUDF2[Geometry, Geometry, java.lang.Boolean](_ within _)
  class ST_Relate     extends NullableUDF2[Geometry, Geometry, String]((g1, g2) => g1.relate(g2).toString)
  class ST_RelateBool extends NullableUDF3[Geometry, Geometry, String, java.lang.Boolean]((g1, g2, pattern) =>
    g1.relate(g2, pattern)
  )

  class ST_Area extends NullableUDF1[Geometry, java.lang.Double](_.getArea)
  class ST_Centroid extends NullableUDF1[Geometry, Point](_.getCentroid)
  class ST_ClosestPoint extends NullableUDF2[Geometry, Geometry, Point](closestPoint)
  class ST_Distance extends NullableUDF2[Geometry, Geometry, java.lang.Double](_ distance _)
  class ST_DistanceSphere extends NullableUDF2[Geometry, Geometry, java.lang.Double]((s, e) =>
    fastDistance(s.getCoordinate, e.getCoordinate)
  )
  class ST_Length extends NullableUDF1[Geometry, java.lang.Double](_.getLength)

  // Assumes input is two points, for use with collect_list and window functions
  class ST_AggregateDistanceSphere extends NullableUDF1[Seq[Geometry], java.lang.Double](a =>
    fastDistance(a.head.getCoordinate, a(1).getCoordinate)
  )

  class ST_LengthSphere extends NullableUDF1[LineString, java.lang.Double](line =>
    line.getCoordinates.sliding(2).map { case Array(l, r) => fastDistance(l, r) }.sum
  )

  class ST_Intersection extends NullableUDF2[Geometry, Geometry, Geometry](_ intersection _)
  class ST_Difference extends NullableUDF2[Geometry, Geometry, Geometry](_ difference _)

  val ST_Contains   = new ST_Contains()
  val ST_Covers     = new ST_Covers()
  val ST_Crosses    = new ST_Crosses()
  val ST_Disjoint   = new ST_Disjoint()
  val ST_Equals     = new ST_Equals()
  val ST_Intersects = new ST_Intersects()
  val ST_Overlaps   = new ST_Overlaps()
  val ST_Touches    = new ST_Touches()
  val ST_Within     = new ST_Within()
  val ST_Relate     = new ST_Relate()
  val ST_RelateBool = new ST_RelateBool()

  val ST_Area = new ST_Area()
  val ST_Centroid = new ST_Centroid()
  val ST_ClosestPoint = new ST_ClosestPoint()
  val ST_Distance = new ST_Distance()
  val ST_DistanceSphere = new ST_DistanceSphere()
  val ST_Length = new ST_Length()
  val ST_AggregateDistanceSphere = new ST_AggregateDistanceSphere()
  val ST_LengthSphere = new ST_LengthSphere()
  val ST_Intersection = new ST_Intersection()
  val ST_Difference = new ST_Difference()

  // Geometry Processing
  private[geomesa] val ch = new ConvexHull

  override def udfs: Seq[Registerable] =
    Seq(
      ST_Translate,
      ST_Contains,
      ST_Covers,
      ST_Crosses,
      ST_Disjoint,
      ST_Equals,
      ST_Intersects,
      ST_Overlaps,
      ST_Touches,
      ST_Within,
      ST_Relate,
      ST_RelateBool,
      ST_Area,
      ST_ClosestPoint,
      ST_Centroid,
      ST_Distance,
      ST_Length,
      ST_DistanceSphere,
      ST_AggregateDistanceSphere,
      ST_LengthSphere,
      ch,
      ST_Intersection,
      ST_Difference
    )

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
