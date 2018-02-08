/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.spark

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.spark.SQLFunctionHelper._
import org.apache.spark.sql.{Column, SQLContext, TypedColumn}
import org.geotools.geojson.geom.GeometryJSON
import org.locationtech.geomesa.spark.SparkDefaultEncoders._

object SQLGeometricOutputFunctions {
  // use ThreadLocal to ensure thread safety
  private val geomJSON = new ThreadLocal[GeometryJSON]() {
    override def initialValue() = new GeometryJSON()
  }
  val ST_AsBinary: Geometry => Array[Byte] = nullableUDF(geom => WKBUtils.write(geom))
  val ST_AsGeoJSON: Geometry => String = nullableUDF(geom => geomJSON.get().toString(geom))
  val ST_AsLatLonText: Point => String = nullableUDF(point => toLatLonString(point))
  val ST_AsText: Geometry => String = nullableUDF(geom => geom.toText)

  private[geomesa] val namer = Map(
    ST_AsBinary -> "st_asBinary",
    ST_AsGeoJSON -> "st_asGeoJSON",
    ST_AsLatLonText -> "st_asLatLonText",
    ST_AsText -> "st_asText"
  )

  def st_asBinary(geom: Column): TypedColumn[Any, Array[Byte]] =
    udfToColumn(ST_AsBinary, namer, geom)
  def st_asBinary(geom: Geometry): TypedColumn[Any, Array[Byte]] =
    udfToColumnLiterals(ST_AsBinary, namer, geom)

  def st_asGeoJSON(geom: Column): TypedColumn[Any, String] =
    udfToColumn(ST_AsGeoJSON, namer, geom)
  def st_asGeoJSON(geom: Geometry): TypedColumn[Any, String] =
    udfToColumnLiterals(ST_AsGeoJSON, namer, geom)

  def st_asLatLonText(point: Column): TypedColumn[Any, String] =
    udfToColumn(ST_AsLatLonText, namer, point)
  def st_asLatLonText(point: Point): TypedColumn[Any, String] =
    udfToColumnLiterals(ST_AsLatLonText, namer, point)

  def st_asText(geom: Column): TypedColumn[Any, String] =
    udfToColumn(ST_AsText, namer, geom)
  def st_asText(geom: Geometry): TypedColumn[Any, String] =
    udfToColumnLiterals(ST_AsText, namer, geom).as[String]

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(namer(ST_AsBinary), ST_AsBinary)
    sqlContext.udf.register(namer(ST_AsGeoJSON), ST_AsGeoJSON)
    sqlContext.udf.register(namer(ST_AsLatLonText), ST_AsLatLonText)
    sqlContext.udf.register(namer(ST_AsText), ST_AsText)
  }

  private def toLatLonString(point: Point): String = {
    val coordinate = point.getCoordinate
    s"${latLonFormat(coordinate.y, lat = true)} ${latLonFormat(coordinate.x, lat = false)}"
  }

  private def latLonFormat(value: Double, lat: Boolean): String = {
    val degrees = value.floor
    val decimal = value - degrees
    val minutes = (decimal * 60).floor
    val seconds = (decimal * 60 - minutes) * 60
    if (lat)
      f"${degrees.abs}%1.0f\u00B0$minutes%1.0f" +"\'" + f"$seconds%1.3f" + "\"" + s"${if (degrees < 0) "S" else "N"}"
    else
      f"${degrees.abs}%1.0f\u00B0$minutes%1.0f" +"\'" + f"$seconds%1.3f" + "\"" + s"${if (degrees < 0) "W" else "E"}"
  }
}
