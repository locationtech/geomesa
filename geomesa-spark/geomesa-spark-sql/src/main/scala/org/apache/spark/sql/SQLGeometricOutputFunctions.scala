/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.text.WKBUtils


object SQLGeometricOutputFunctions {
  val ST_AsBinary: Geometry => Array[Byte] = geom => WKBUtils.write(geom)
  val ST_AsGeoJSON: Geometry => String = geom => toGeoJson(geom)
  val ST_AsLatLonText: Geometry => String = geom => toLatLonString(geom)
  val ST_AsText: Geometry => String = geom => geom.toText
  val ST_GeoHash: (Geometry, Int) => String = (geom, prec) => GeoHash(geom.getInteriorPoint, prec).hash

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_asBinary", ST_AsBinary)
    sqlContext.udf.register("st_asGeoJSON", ST_AsGeoJSON)
    sqlContext.udf.register("st_asLatLonText", ST_AsLatLonText)
    sqlContext.udf.register("st_asText", ST_AsText)
    sqlContext.udf.register("st_geoHash", ST_GeoHash)
  }

  private def toGeoJson(geom: Geometry): String = {
    val geoString = geom.toText
    val parenthesis = geoString.indexOf('(')
    val geomType = geoString.substring(0, parenthesis).trim
    val formattedCoords = formatCoordinates(geoString.substring(parenthesis).trim, geomType == "POINT")
    "{\"type\":\"" + s"${formatType(geomType)}" + "\",\"coordinates\":" + s"$formattedCoords}"
 }

  private def toLatLonString(g: Geometry): String = {
    val coordinate = g.getCoordinate
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

  private def formatType(geomType: String): String = {
    geomType match {
      case "POINT" => "Point"
      case "LINESTRING" => "LineString"
      case "POLYGON" => "Polygon"
      case "MULTIPOINT" => "MultiPoint"
      case "MULTILINESTRING" => "MultiLineString"
      case "MULTIPOLYGON" => "MultiPolygon"
    }
  }

  private def formatCoordinates(string: String, isPoint: Boolean): String = {
    var formattedString = ""
    if (!isPoint) formattedString += "["
    val stringArr = string.replace('(', '[').replace(')', ']').split(", ")
    for (i <- 0 until stringArr.length) {
      formattedString += stringArr(i).replace(' ', ',')
      if (!(i == stringArr.length - 1)) formattedString += "],["
    }
    if (!isPoint) formattedString += "]"
    formattedString
  }
}
