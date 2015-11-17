/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process.rangeHistogram

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.util.NullProgressListener
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.RangeHistogramIterator.createFeatureType
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature

@DescribeProcess(
  title = "Range Histogram Process",
  description = "Returns a histogram of how many data points fall in different buckets within an interval."
)
class RangeHistogramProcess extends Logging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "The feature set on which to query")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "intervalStart",
                 description = "The start of the interval")
               intervalStart: Long,

               @DescribeParameter(
                 name = "intervalEnd",
                 description = "The end of the interval")
               intervalEnd: Long,

               @DescribeParameter(
                 name = "buckets",
                 min = 1,
                 description = "How many buckets we want in our histogram")
               buckets: Int,

               @DescribeParameter(
                 name = "attribute",
                 description = "The attribute name to run the histogram against")
               attribute: String

               ): SimpleFeatureCollection = {

    logger.debug("Attempting Geomesa range histogram process on type " + features.getClass.getName)

    if (features.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val interval: com.google.common.collect.Range[java.lang.Long] = com.google.common.collect.Ranges.closed(intervalStart, intervalEnd)
    val visitor = new RangeHistogramVisitor(features, interval, buckets, attribute)
    features.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[RangeHistogramResult].results
  }
}

class RangeHistogramVisitor(features: SimpleFeatureCollection, interval: com.google.common.collect.Range[java.lang.Long], buckets: Int, attribute: String)
  extends FeatureCalc with Logging {

  val retType = createFeatureType(features.getSchema())
  val manualVisitResults = new DefaultFeatureCollection(null, retType)

  //  Called for non AccumuloFeatureCollections
  def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    manualVisitResults.add(sf)
  }

  var resultCalc: RangeHistogramResult = new RangeHistogramResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = RangeHistogramResult(r)

  def query(source: SimpleFeatureSource, query: Query) = {
    logger.debug("Running Geomesa range histogram process on source type " + source.getClass.getName)
    query.getHints.put(QueryHints.RANGE_HISTOGRAM_KEY, java.lang.Boolean.TRUE)
    query.getHints.put(QueryHints.RANGE_HISTOGRAM_INTERVAL_KEY, interval)
    query.getHints.put(QueryHints.RANGE_HISTOGRAM_BUCKETS_KEY, buckets)
    query.getHints.put(QueryHints.RANGE_HISTOGRAM_ATTRIBUTE, attribute)
    source.getFeatures(query)
  }
}

case class RangeHistogramResult(results: SimpleFeatureCollection) extends AbstractCalcResult
