/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.util

import java.{lang => jl}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.geojson.GeoJsonWriter

class RowGeoJSON(structType: StructType, geomOrdinal: Int) {

  val geomJson = new GeoJsonWriter()
  geomJson.setEncodeCRS(false)

  def toGeoJSON(row: Row): String = {
    val sb = new jl.StringBuilder()

    sb.append(""" {"type": "Feature", "geometry": """) // start feature
    val geometry = row.getAs[Geometry](geomOrdinal)
    if (geometry != null) {
      sb.append(geomJson.write(row.getAs[Geometry](geomOrdinal))) // write geometry
    } else {
      sb.append("null")
    }

    sb.append(""", "properties":{ """) // start properties

    var i = 0
    var written = false
    structType.fields.foreach { sf =>
      if (i != geomOrdinal) {
        written = true
        sb.append(s"""  "${sf.name}": "${row.get(i)}",""")
      }
      i += 1
    }

    // remove extra comma
    if (written) {
      sb.setLength(sb.length() - 1)
    }

    sb.append("}") // close properties
    sb.append("}") // close feature

    sb.toString
  }
}
