/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process.stats

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.util.NullProgressListener
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.StatsIterator.createFeatureType
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature

@DescribeProcess(
  title = "Stats Iterator Process",
  description = "Returns stats based upon the passed in stats string"
)
class StatsIteratorProcess extends Logging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "The feature set on which to query")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "statString",
                 description = "The string indicating what stats to instantiate")
               statString: String

               ): SimpleFeatureCollection = {

    logger.debug("Attempting Geomesa stats iterator process on type " + features.getClass.getName)

    if (features.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val visitor = new StatsVisitor(features, statString)
    features.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[StatsIteratorResult].results
  }
}

class StatsVisitor(features: SimpleFeatureCollection, statString: String)
  extends FeatureCalc with Logging {

  val retType = createFeatureType(features.getSchema())
  val manualVisitResults = new DefaultFeatureCollection(null, retType)

  //  Called for non AccumuloFeatureCollections
  def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    manualVisitResults.add(sf)
  }

  var resultCalc: StatsIteratorResult = new StatsIteratorResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = StatsIteratorResult(r)

  def query(source: SimpleFeatureSource, query: Query) = {
    logger.debug("Running Geomesa stats iterator process on source type " + source.getClass.getName)
    query.getHints.put(QueryHints.STATS_STRING, statString)
    source.getFeatures(query)
  }
}

case class StatsIteratorResult(results: SimpleFeatureCollection) extends AbstractCalcResult
