/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.SQLContext
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.referencing.{CRS, GeodeticCalculator}
import org.locationtech.geomesa.spark.jts.udf.NullableUDF._
import org.locationtech.geomesa.spark.jts.udf.UDFFactory
import org.locationtech.geomesa.spark.jts.udf.UDFFactory.Registerable
import org.locationtech.jts.geom.{Coordinate, Geometry, LineString}

object GeometricDistanceFunctions extends UDFFactory {

  class ST_DistanceSpheroid extends NullableUDF2[Geometry, Geometry, java.lang.Double]((s, e) =>
    fastDistance(s.getCoordinate, e.getCoordinate))

  // Assumes input is two points, for use with collect_list and window functions
  class ST_AggregateDistanceSpheroid extends NullableUDF1[Seq[Geometry], java.lang.Double](a =>
    fastDistance(a.head.getCoordinate, a(1).getCoordinate)
  )

  class ST_LengthSpheroid extends NullableUDF1[LineString, java.lang.Double](line =>
    line.getCoordinates.sliding(2).map { case Array(l, r) => fastDistance(l, r) }.sum
  )

  class ST_Transform extends NullableUDF3[Geometry, String, String, Geometry]((geometry, fromCRSCode, toCRSCode) => {
    val transformer = new GeometryCoordinateSequenceTransformer
    val fromCode = CRS.decode(fromCRSCode, true)
    val toCode = CRS.decode(toCRSCode, true)
    transformer.setMathTransform(CRS.findMathTransform(fromCode, toCode, true))
    transformer.transform(geometry)
  })

  val ST_DistanceSpheroid = new ST_DistanceSpheroid()

  // Assumes input is two points, for use with collect_list and window functions
  val ST_AggregateDistanceSpheroid = new ST_AggregateDistanceSpheroid()

  val ST_LengthSpheroid = new ST_LengthSpheroid()

  val ST_Transform = new ST_Transform()

  def registerFunctions(sqlContext: SQLContext): Unit = {

  }

  @transient private val geoCalcs = new ThreadLocal[GeodeticCalculator] {
    override def initialValue(): GeodeticCalculator = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
  }

  def fastDistance(c1: Coordinate, c2: Coordinate): Double = {
    val calc = geoCalcs.get()
    calc.setStartingGeographicPoint(c1.x, c1.y)
    calc.setDestinationGeographicPoint(c2.x, c2.y)
    calc.getOrthodromicDistance
  }

  override def udfs: Seq[Registerable] =
    Seq(
      ST_DistanceSpheroid,
      ST_AggregateDistanceSpheroid,
      ST_LengthSpheroid,
      ST_Transform
    )
}
