/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.udf.NullableUDF._
import org.locationtech.geomesa.spark.jts.udf.UDFFactory.Registerable
import org.locationtech.geomesa.spark.jts.util.{GeoHashUtils, WKBUtils}
import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.jts.io.geojson.GeoJsonWriter

object GeometricOutputFunctions extends UDFFactory {

  // use ThreadLocal to ensure thread safety
  private val geomJSON = new ThreadLocal[GeoJsonWriter]() {
    override def initialValue(): GeoJsonWriter = {
      val writer = new GeoJsonWriter()
      writer.setEncodeCRS(false)
      writer
    }
  }

  class ST_AsBinary extends NullableUDF1[Geometry, Array[Byte]](WKBUtils.write)
  class ST_AsGeoJSON extends NullableUDF1[Geometry, String](toGeoJson)
  class ST_AsLatLonText extends NullableUDF1[Point, String](toLatLonString)
  class ST_AsText extends NullableUDF1[Geometry, String](_.toText)
  class ST_GeoHash extends NullableUDF2[Geometry, Int, String](GeoHashUtils.encode)

  val ST_AsBinary = new ST_AsBinary()
  val ST_AsGeoJSON = new ST_AsGeoJSON()
  val ST_AsLatLonText = new ST_AsLatLonText()
  val ST_AsText = new ST_AsText()
  val ST_GeoHash = new ST_GeoHash()

  override def udfs: Seq[Registerable] =
    Seq(
      ST_AsBinary,
      ST_AsGeoJSON,
      ST_AsLatLonText,
      ST_AsText,
      ST_GeoHash
    )

  private def toGeoJson(g: Geometry): String = geomJSON.get().write(g)

  private def toLatLonString(point: Point): String = {
    val coordinate = point.getCoordinate
    s"${latLonFormat(coordinate.y, lat = true)} ${latLonFormat(coordinate.x, lat = false)}"
  }

  private def latLonFormat(value: Double, lat: Boolean): String = {
    val degrees = value.floor
    val decimal = value - degrees
    val minutes = (decimal * 60).floor
    val seconds = (decimal * 60 - minutes) * 60
    if (lat) {
      f"${degrees.abs}%1.0f\u00B0$minutes%1.0f" +"\'" + f"$seconds%1.3f" + "\"" + s"${if (degrees < 0) "S" else "N"}"
    } else {
      f"${degrees.abs}%1.0f\u00B0$minutes%1.0f" +"\'" + f"$seconds%1.3f" + "\"" + s"${if (degrees < 0) "W" else "E"}"
    }
  }
}
