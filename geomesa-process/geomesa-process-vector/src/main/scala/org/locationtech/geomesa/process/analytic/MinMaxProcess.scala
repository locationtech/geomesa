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
import org.locationtech.geomesa.index.process.MinMaxVisitor
import org.locationtech.geomesa.process.GeoMesaProcess

@DescribeProcess(
  title = "Min/Max Process",
  description = "Gets attribute bounds for a data set"
)
class MinMaxProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
                 @DescribeParameter(
                   name = "features",
                   description = "The feature set on which to query")
                 features: SimpleFeatureCollection,

                 @DescribeParameter(
                   name = "attribute",
                   description = "The attribute to gather bounds for")
                 attribute: String,

                 @DescribeParameter(
                   name = "cached",
                   description = "Return cached values, if available",
                   min = 0, max = 1)
                 cached: java.lang.Boolean = null

             ): SimpleFeatureCollection = {

    require(attribute != null, "Attribute is a required field")

    logger.debug(s"Attempting min/max process on type ${features.getClass.getName}")

    val visitor = new MinMaxVisitor(features, attribute, Option(cached).forall(_.booleanValue()))
    GeoMesaFeatureCollection.visit(features, visitor)
    visitor.getResult.results
  }
}
