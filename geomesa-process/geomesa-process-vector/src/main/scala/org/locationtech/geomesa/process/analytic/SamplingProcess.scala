/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.utils.FeatureSampler
import org.locationtech.geomesa.process.{FeatureResult, GeoMesaProcess, GeoMesaProcessVisitor}
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.util.ProgressListener

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

class SamplingVisitor(features: SimpleFeatureCollection, percent: Float, threading: Option[String])
    extends GeoMesaProcessVisitor with LazyLogging {

  private val manualVisitResults = new ListFeatureCollection(features.getSchema)

  private var resultCalc = FeatureResult(manualVisitResults)

  private val nth = (1 / percent.toFloat).toInt
  private val thread = threading.map(features.getSchema.indexOf).filter(_ != -1)

  private val sampling = FeatureSampler.sample(nth, thread)

  // non-optimized visit
  override def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    if (sampling(sf)) {
      manualVisitResults.add(sf)
    }
  }

  override def getResult: FeatureResult = resultCalc

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Running Geomesa sampling process on source type ${source.getClass.getName}")
    query.getHints.put(QueryHints.SAMPLING, percent)
    threading.foreach(query.getHints.put(QueryHints.SAMPLE_BY, _))
    resultCalc = FeatureResult(source.getFeatures(query))
  }
}
