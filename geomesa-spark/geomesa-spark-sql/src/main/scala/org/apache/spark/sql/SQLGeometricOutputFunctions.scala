/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.apache.spark.sql.SQLFunctionHelper.nullableUDF
import org.geotools.geojson.geom.GeometryJSON
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.text.WKBUtils

object SQLGeometricOutputFunctions {
  // use ThreadLocal to ensure thread safety
  private val geomJSON = new ThreadLocal[GeometryJSON]() {
    override def initialValue() = new GeometryJSON()
  }
  val ST_AsBinary: Geometry => Array[Byte] = nullableUDF(geom => WKBUtils.write(geom))
  val ST_AsGeoJSON: Geometry => String = nullableUDF(geom => geomJSON.get().toString(geom))
  val ST_AsLatLonText: Point => String = nullableUDF(point => toLatLonString(point))
  val ST_AsText: Geometry => String = nullableUDF(geom => geom.toText)
  val ST_GeoHash: (Geometry, Int) => String =
    nullableUDF((geom, prec) => GeoHash(geom.getInteriorPoint, prec).hash)

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_asBinary", ST_AsBinary)
    sqlContext.udf.register("st_asGeoJSON", ST_AsGeoJSON)
    sqlContext.udf.register("st_asLatLonText", ST_AsLatLonText)
    sqlContext.udf.register("st_asText", ST_AsText)
    sqlContext.udf.register("st_geoHash", ST_GeoHash)
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
