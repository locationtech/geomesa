/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.util

import java.{lang => jl}

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.geojson.GeoJsonWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types._

class RowGeoJSON(structType: StructType, var geomOrdinal: Option[Int] = None) {

  val geomJson = new GeoJsonWriter()
  geomJson.setEncodeCRS(false)

  if (geomOrdinal.isEmpty) {
    setGeometryOrdinal()
  }

  def setGeometryOrdinal() : Unit = {
    val foundOrdinal = structType.fields.indexWhere(sf => {
      JTSTypes.typeMap.values.exists(_.equals(sf.dataType.getClass))
    })

    if (foundOrdinal == -1) {
      throw new IllegalArgumentException("Provided schema does not have a geometry type")
    } else {
      geomOrdinal = Some(foundOrdinal)
    }
  }

  def toString(row: Row): String = {
    val sb = new jl.StringBuilder()

    sb.append(""" {"type": "Feature", "geometry": """) // start feature
    val geometry = row.getAs[Geometry](geomOrdinal.get)
    if (geometry != null) {
      sb.append(geomJson.write(row.getAs[Geometry](geomOrdinal.get))) // write geometry
    } else {
      sb.append(""" "null" """)
    }

    sb.append(""", "properties":{ """) // start properties

    var i = 0 //1 // start at 1 to skip fid
    var written = false
    structType.fields.foreach { sf =>
      if (i != geomOrdinal.get) {
        written = true
        sb.append(s"""  "${sf.name}": "${row.get(i)}",""")
      }
      i += 1
    }

    // remove extra comma
    if (written) {
      sb.setLength(sb.length() - 1)
    }

    sb.append("},") // close properties
    sb.append(s""" "id": "${row.get(0)}" """) // add fid
    sb.append("}") // close feature

    sb.toString
  }

}
