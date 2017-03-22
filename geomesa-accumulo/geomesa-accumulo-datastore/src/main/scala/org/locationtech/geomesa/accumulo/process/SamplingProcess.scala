/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.process

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.locationtech.geomesa.accumulo.iterators.SamplingIterator
import org.locationtech.geomesa.index.conf.QueryHints
import org.opengis.coverage.grid.GridGeometry
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
class SamplingProcess extends VectorProcess with LazyLogging {

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

    if (data.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val visitor = new SamplingVisitor(data, samplePercent, Option(threadBy))
    data.accepts(visitor, monitor)
    visitor.getResult.asInstanceOf[SamplingResult].results
  }

  /**
    * Note that in order to pass validation, all parameters named here must also appear in the
    * parameter list of the <tt>execute</tt> method, even if they are not used there.
    *
    * @param samplePercent percent of features to return, in the range (0, 1)
    * @param threadBy threading field, used to group sampling of features
    * @param targetQuery query to modify
    * @param targetGridGeometry the grid geometry of the destination image
    * @throws org.geotools.process.ProcessException if something goes wrong
    * @return the transformed query
    */
  @throws(classOf[ProcessException])
  def invertQuery(@DescribeParameter(name = "samplePercent", description = "Percent of features to return, between 0 and 1", minValue = 0, maxValue = 1)
                  samplePercent: Float,
                  @DescribeParameter(name = "threadBy", description = "Attribute field to link associated features for sampling", min = 0)
                  threadBy: String,
                  targetQuery: Query,
                  targetGridGeometry: GridGeometry): Query = {
    val invertedQuery = new Query(targetQuery)
    invertedQuery.getHints.put(QueryHints.SAMPLING, samplePercent)
    if (threadBy != null) {
      invertedQuery.getHints.put(QueryHints.SAMPLE_BY, threadBy)
    }
    invertedQuery
  }
}

class SamplingVisitor(features: SimpleFeatureCollection, percent: Float, threading: Option[String])
    extends FeatureCalc with LazyLogging {

  private val manualVisitResults = new DefaultFeatureCollection(null, features.getSchema)

  private var resultCalc: SamplingResult = new SamplingResult(manualVisitResults)

  private val nth = (1 / percent.toFloat).toInt
  private val thread = threading.map(features.getSchema.indexOf).filter(_ != -1)

  private val sampling = SamplingIterator.sample(nth, thread)

  // Called for non AccumuloFeactureCollections
  override def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    if (sampling(sf)) {
      manualVisitResults.add(sf)
    }
  }

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection): Unit = resultCalc = SamplingResult(r)

  def sample(source: SimpleFeatureSource, query: Query): SimpleFeatureCollection = {
    logger.debug(s"Running Geomesa sampling process on source type ${source.getClass.getName}")
    source.getFeatures(query)
  }
}

case class SamplingResult(results: SimpleFeatureCollection) extends AbstractCalcResult
