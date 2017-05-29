/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.process.query

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.util.NullProgressListener
import org.locationtech.geomesa.process.{GeoMesaProcess, GeoMesaProcessVisitor}
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

@DescribeProcess(
  title = "Geomesa Query",
  description = "Performs a Geomesa optimized query using spatiotemporal indexes"
)
class QueryProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "The feature set on which to query")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "filter",
                 min = 0,
                 description = "The filter to apply to the features collection")
               filter: Filter,

               @DescribeParameter(
                 name = "properties",
                 min = 0,
                 description = "The lists of properties and transform definitions to apply")
               properties: String = null
             ): SimpleFeatureCollection = {

    logger.debug("Attempting Geomesa query on type " + features.getClass.getName)

    val arrayString = Option(properties).map(_.split(";")).orNull

    val visitor = new QueryVisitor(features, Option(filter).getOrElse(Filter.INCLUDE), arrayString)
    features.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[QueryResult].results
  }
}

class QueryVisitor(features: SimpleFeatureCollection,
                   filter: Filter,
                   properties: Array[String] = null) extends GeoMesaProcessVisitor with LazyLogging {

  val manualVisitResults = new DefaultFeatureCollection(null, features.getSchema)
  val ff  = CommonFactoryFinder.getFilterFactory2

  // Called for non AccumuloFeactureCollections
  def visit(feature: Feature): Unit = {
    // TODO:  GEOMESA-1755 Add any necessary transform support to the QueryProcess
    val sf = feature.asInstanceOf[SimpleFeature]
    if(filter.evaluate(sf)) {
      manualVisitResults.add(sf)
    }
  }

  var resultCalc: QueryResult = QueryResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug("Running Geomesa query on source type "+source.getClass.getName)
    val combinedFilter = ff.and(query.getFilter, filter)
    query.setFilter(combinedFilter)
    if (properties != null) {
      if (query.getProperties != Query.ALL_PROPERTIES) {
        logger.warn(s"Overriding inner query's properties (${query.getProperties}) with properties / transforms $properties.")
      }
      query.setPropertyNames(properties)
    }
    resultCalc = QueryResult(source.getFeatures(query))
  }
}

case class QueryResult(results: SimpleFeatureCollection) extends AbstractCalcResult