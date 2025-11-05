/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.util.ProgressListener
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.process.SamplingVisitor
import org.locationtech.geomesa.process.GeoMesaProcess

/**
  * Returns a reduced set of features using statistical sampling.
  */
@DescribeProcess(
  title = "Sampling Process",
  description = "Uses statistical sampling to reduces the features returned by a query"
)
class SamplingProcess extends GeoMesaProcess with LazyLogging {

  /**
    * Reduces the feature collection by statistically sampling the features.
    *
    * @param data input feature collection
    * @param samplePercent percent of features to return, in the range (0, 1)
    * @param threadBy threading field, used to group sampling of features
    * @param monitor listener to monitor progress
    * @throws org.geotools.process.ProcessException if something goes wrong
    * @return
    */
  @throws(classOf[ProcessException])
  @DescribeResult(name = "result", description = "Output features")
  def execute(@DescribeParameter(name = "data", description = "Input features")
              data: SimpleFeatureCollection,
              @DescribeParameter(name = "samplePercent", description = "Percent of features to return, between 0 and 1", minValue = 0, maxValue = 1)
              samplePercent: Float,
              @DescribeParameter(name = "threadBy", description = "Attribute field to link associated features for sampling", min = 0)
              threadBy: String,
              monitor: ProgressListener): SimpleFeatureCollection = {

    logger.trace(s"Attempting sampling on ${data.getClass.getName}")

    val visitor = new SamplingVisitor(data, samplePercent, Option(threadBy))
    GeoMesaFeatureCollection.visit(data, visitor, monitor)
    visitor.getResult.results
  }
}
