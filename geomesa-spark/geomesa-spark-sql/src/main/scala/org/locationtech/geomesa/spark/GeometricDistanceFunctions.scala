/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.{Encoder, Encoders, SQLContext}
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.referencing.{CRS, GeodeticCalculator}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper.{NullableUDF1, NullableUDF2, NullableUDF3}
import org.locationtech.jts.geom.{Coordinate, Geometry, LineString}

object GeometricDistanceFunctions extends SpatialEncoders {

  implicit def stringEncoder: Encoder[String] = Encoders.STRING
  implicit def jDoubleEncoder: Encoder[java.lang.Double] = Encoders.DOUBLE

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
    sqlContext.udf.register(ST_DistanceSpheroid.name, ST_DistanceSpheroid)
    sqlContext.udf.register(ST_AggregateDistanceSpheroid.name, ST_AggregateDistanceSpheroid)
    sqlContext.udf.register(ST_LengthSpheroid.name, ST_LengthSpheroid)
    sqlContext.udf.register(ST_Transform.name, ST_Transform)
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


}
