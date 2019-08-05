/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import java.io.StringWriter

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.spark.SparkUtils

object GeoJSONExtensions {

  implicit private val encoder: Encoder[String] = org.apache.spark.sql.Encoders.STRING

  implicit class GeoJSONDataFrame(val df: DataFrame) extends AnyVal {

    /**
      * Convert the dataframe into geojson rows
      *
      * @return
      */
    def toGeoJSON: Dataset[String] = {
      val schema = df.schema // note: needs to be outside mapPartitions, as otherwise it ends up null...
      df.mapPartitions { iter =>
        if (iter.isEmpty) { Iterator.empty } else {
          val json = new FeatureJSON()
          val writer = new StringWriter()
          val mappings = SparkUtils.rowsToFeatures("", schema)
          iter.map { row =>
            writer.getBuffer.setLength(0)
            json.writeFeature(mappings.apply(row), writer)
            writer.toString
          }
        }
      }
    }
  }
}
