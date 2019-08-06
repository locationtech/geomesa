/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.knn

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.process.{GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.jts.geom.Point
import org.opengis.feature.Feature

import scala.collection.JavaConverters._


@DescribeProcess(
  title = "Geomesa-enabled K Nearest Neighbor Search",
  description = "Performs a K Nearest Neighbor search on a Geomesa feature collection using another feature collection as input"
)
class KNearestNeighborSearchProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "inputFeatures",
                 description = "Input feature collection that defines the KNN search")
               inputFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "dataFeatures",
                 description = "The data set to query for matching features")
               dataFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "numDesired",
                 description = "K: number of nearest neighbors to return")
               numDesired: java.lang.Integer,

               @DescribeParameter(
                 name = "estimatedDistance",
                 description = "Estimate of Search Distance in meters for K neighbors---used to set the granularity of the search")
               estimatedDistance: java.lang.Double,

               @DescribeParameter(
                 name = "maxSearchDistance",
                 description = "Maximum search distance in meters---used to prevent runaway queries of the entire table")
               maxSearchDistance: java.lang.Double
               ): SimpleFeatureCollection = {

    logger.debug("Attempting Geomesa K-Nearest Neighbor Search on collection type " + dataFeatures.getClass.getName)

    val visitor = new KNNVisitor(inputFeatures, dataFeatures, numDesired, estimatedDistance, maxSearchDistance)
    GeoMesaFeatureCollection.visit(dataFeatures, visitor)
    visitor.getResult.asInstanceOf[KNNResult].results
  }
}

/**
 *  The main visitor class for the KNN search process
 */

class KNNVisitor( inputFeatures:     SimpleFeatureCollection,
                  dataFeatures:      SimpleFeatureCollection,
                  numDesired:        java.lang.Integer,
                  estimatedDistance: java.lang.Double,
                  maxSearchDistance: java.lang.Double
                ) extends GeoMesaProcessVisitor with LazyLogging {

  val manualVisitResults = new DefaultFeatureCollection(null, dataFeatures.getSchema)

  // called for non AccumuloFeatureCollections
  // FIXME Implement as detailed in  GEOMESA-284
  def visit(feature: Feature): Unit = {}

  var resultCalc: KNNResult = KNNResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  /** The KNN-Search interface for the WPS process.
    *
    * Takes as input a Query and SimpleFeatureSource, in addition to
    * inputFeatures which define one or more SimpleFeatures for which to find KNN of each
    *
    * Note that the results are NOT de-duplicated!
    *
    */
  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug("Running Geomesa K-Nearest Neighbor Search on source type " + source.getClass.getName)

    // create a new Feature collection to hold the results of the KNN search around each point
    val resultCollection = new DefaultFeatureCollection
    val searchFeatureIterator = SelfClosingIterator(inputFeatures.features())

    // for each entry in the inputFeatures collection:
    while (searchFeatureIterator.hasNext) {
      val aFeatureForSearch = searchFeatureIterator.next()
      aFeatureForSearch.getDefaultGeometry match {
        case geo: Point =>
          val knnResults = KNNQuery.runNewKNNQuery(source, query, numDesired, estimatedDistance, maxSearchDistance, aFeatureForSearch)
          // extract the SimpleFeatures and convert to a Collection. Ordering will not be preserved.
          val sfList = knnResults.getK.map {_.sf}.asJavaCollection
          resultCollection.addAll(sfList)
        case _ => logger.warn("K Nearest Neighbor Search not implemented for non-point geometries, skipping this Feature")
      }
    }
    resultCalc = KNNResult(resultCollection)
  }
}

case class KNNResult(results: SimpleFeatureCollection) extends AbstractCalcResult
