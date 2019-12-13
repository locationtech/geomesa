/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.locationtech.jts.geom.{Coordinate, Geometry, LineString}
import org.apache.spark.sql.SQLContext
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper.nullableUDF

object GeometricDistanceFunctions {
  import java.{lang => jl}

  val ST_DistanceSpheroid: (Geometry, Geometry) => jl.Double =
    nullableUDF((s, e) => fastDistance(s.getCoordinate, e.getCoordinate))

  // Assumes input is two points, for use with collect_list and window functions
  val ST_AggregateDistanceSpheroid: Seq[Geometry] => jl.Double = a => ST_DistanceSpheroid(a(0), a(1))

  val ST_LengthSpheroid: LineString => jl.Double =
    nullableUDF(line => line.getCoordinates.sliding(2).map { case Array(l, r) => fastDistance(l, r) }.sum)

  private[geomesa] val distanceNames = Map(

    ST_DistanceSpheroid -> "st_distanceSpheroid",
    ST_AggregateDistanceSpheroid -> "st_aggregateDistanceSpheroid",
    ST_LengthSpheroid -> "st_lengthSpheroid"
  )


  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(distanceNames (ST_DistanceSpheroid), ST_DistanceSpheroid)
    sqlContext.udf.register(distanceNames (ST_AggregateDistanceSpheroid), ST_AggregateDistanceSpheroid)
    sqlContext.udf.register(distanceNames (ST_LengthSpheroid), ST_LengthSpheroid)
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
