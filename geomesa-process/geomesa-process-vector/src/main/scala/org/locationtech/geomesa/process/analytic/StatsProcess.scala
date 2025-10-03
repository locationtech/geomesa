/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.process.StatsVisitor
import org.locationtech.geomesa.process.GeoMesaProcess

@DescribeProcess(
  title = "Stats Process",
  description = "Gathers statistics for a data set"
)
class StatsProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
                 @DescribeParameter(
                   name = "features",
                   description = "The feature set on which to query")
                 features: SimpleFeatureCollection,

                 @DescribeParameter(
                   name = "statString",
                   description = "The string indicating what stats to instantiate")
                 statString: String,

                 @DescribeParameter(
                   name = "encode",
                   min = 0,
                   description = "Return the values encoded or as json")
                 encode: java.lang.Boolean = null,

                 @DescribeParameter(
                   name = "properties",
                   min = 0,
                   max = 128,
                   collectionType = classOf[String],
                   description = "The properties / transforms to apply before gathering stats")
                 properties: java.util.List[String] = null

             ): SimpleFeatureCollection = {

    logger.debug(s"Attempting stats iterator process on type ${features.getClass.getName}")

    val propsArray = Option(properties).map(_.toArray(Array.empty[String])).filter(_.length > 0).orNull

    val visitor = new StatsVisitor(features, statString, Option(encode).exists(_.booleanValue()), propsArray)
    GeoMesaFeatureCollection.visit(features, visitor)
    visitor.getResult.results
  }
}
