/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process.temporalDensity

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.util.NullProgressListener
import org.joda.time.Interval
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.TemporalDensityIterator.createFeatureType
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature

@DescribeProcess(
  title = "Temporal Density Process",
  description = "Returns a histogram of how many data points fall in different time buckets within an interval."
)
class TemporalDensityProcess extends Logging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "The feature set on which to query")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "startDate",
                 description = "The start of the time interval")
               startDate: Date,

               @DescribeParameter(
                 name = "endDate",
                 description = "The end of the time interval")
               endDate: Date,

               @DescribeParameter(
                 name = "buckets",
                 min = 1,
                 description = "How many buckets we want to divide our time interval into.")
               buckets: Int

               ): SimpleFeatureCollection = {

    logger.debug("Attempting Geomesa temporal density on type " + features.getClass.getName)

    if (features.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val interval = new Interval(startDate.getTime, endDate.getTime)

    val visitor = new TemporalDensityVisitor(features, interval, buckets)
    features.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[TDResult].results
  }
}

class TemporalDensityVisitor(features: SimpleFeatureCollection, interval: Interval, buckets: Int)
  extends FeatureCalc with Logging {

  val retType = createFeatureType(features.getSchema())
  val manualVisitResults = new DefaultFeatureCollection(null, retType)

  //  Called for non AccumuloFeatureCollections
  def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    manualVisitResults.add(sf)
  }

  var resultCalc: TDResult = new TDResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = TDResult(r)

  def query(source: SimpleFeatureSource, query: Query) = {
    logger.debug("Running Geomesa temporal density process on source type " + source.getClass.getName)
    query.getHints.put(QueryHints.TEMPORAL_DENSITY_KEY, java.lang.Boolean.TRUE)
    query.getHints.put(QueryHints.TIME_INTERVAL_KEY, interval)
    query.getHints.put(QueryHints.TIME_BUCKETS_KEY, buckets)
    source.getFeatures(query)
  }
}

case class TDResult(results: SimpleFeatureCollection) extends AbstractCalcResult
