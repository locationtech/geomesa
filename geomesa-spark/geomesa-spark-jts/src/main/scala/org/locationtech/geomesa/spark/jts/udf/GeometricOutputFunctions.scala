/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.spark.jts.udf

import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.jts.io.geojson.GeoJsonWriter
import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._
import org.locationtech.geomesa.spark.jts.util.WKBUtils
import org.locationtech.geomesa.spark.jts.util.GeoHashUtils._

object GeometricOutputFunctions {
  // use ThreadLocal to ensure thread safety
  private val geomJSON = new ThreadLocal[GeoJsonWriter]() {
    override def initialValue() = {
      val writer = new GeoJsonWriter()
      writer.setEncodeCRS(false)
      writer
    }
  }
  val ST_AsBinary: Geometry => Array[Byte] = nullableUDF(geom => WKBUtils.write(geom))
  val ST_AsGeoJSON: Geometry => String = nullableUDF(geom => geomJSON.get().write(geom))
  val ST_AsLatLonText: Point => String = nullableUDF(point => toLatLonString(point))
  val ST_AsText: Geometry => String = nullableUDF(geom => geom.toText)
  val ST_GeoHash: (Geometry, Int) => String = nullableUDF((geom, prec) => encode(geom, prec))

  private[geomesa] val outputNames = Map(
    ST_AsBinary -> "st_asBinary",
    ST_AsGeoJSON -> "st_asGeoJSON",
    ST_AsLatLonText -> "st_asLatLonText",
    ST_AsText -> "st_asText",
    ST_GeoHash -> "st_geoHash"
  )

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(outputNames(ST_AsBinary), ST_AsBinary)
    sqlContext.udf.register(outputNames(ST_AsGeoJSON), ST_AsGeoJSON)
    sqlContext.udf.register(outputNames(ST_AsLatLonText), ST_AsLatLonText)
    sqlContext.udf.register(outputNames(ST_AsText), ST_AsText)
    sqlContext.udf.register(outputNames(ST_GeoHash), ST_GeoHash)
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
