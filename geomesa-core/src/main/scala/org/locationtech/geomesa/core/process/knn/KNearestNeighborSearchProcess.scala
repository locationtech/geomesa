/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.process.knn

import com.vividsolutions.jts.geom.Point

import collection.JavaConverters._
import org.locationtech.geomesa.core.data.AccumuloFeatureCollection
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureIterator
import org.apache.log4j.Logger
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{CalcResult, FeatureCalc, AbstractCalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeResult, DescribeProcess}
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature


@DescribeProcess(
  title = "Geomesa-enabled K Nearest Neighbor Search",
  description = "Performs a K Nearest Neighbor search on a Geomesa feature collection using another feature collection as input"
)
class KNearestNeighborSearchProcess {

  private val log = Logger.getLogger(classOf[KNearestNeighborSearchProcess])

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

    log.info("Attempting Geomesa K-Nearest Neighbor Search on collection type " + dataFeatures.getClass.getName)

    if(!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided data feature collection type may not support geomesa KNN search: "+dataFeatures.getClass.getName)
    }
    if(dataFeatures.isInstanceOf[ReTypingFeatureCollection]) {
      log.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }
    val visitor = new KNNVisitor(inputFeatures, dataFeatures, numDesired, estimatedDistance, maxSearchDistance)
    dataFeatures.accepts(visitor, new NullProgressListener)
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
                ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[KNNVisitor])

  val manualVisitResults = new DefaultFeatureCollection(null, dataFeatures.getSchema)

  // called for non AccumuloFeatureCollections
  // FIXME Implement as detailed in  GEOMESA-284
  def visit(feature: Feature): Unit = {}

  var resultCalc: KNNResult = new KNNResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = KNNResult(r)

  /** The KNN-Search interface for the WPS process.
    *
    * Takes as input a Query and SimpleFeatureSource, in addition to
    * inputFeatures which define one or more SimpleFeatures for which to find KNN of each
    *
    * Note that the results are NOT de-duplicated!
    *
    */
  def kNNSearch(source: SimpleFeatureSource, query: Query) = {
    log.info("Running Geomesa K-Nearest Neighbor Search on source type " + source.getClass.getName)

    // create a new Feature collection to hold the results of the KNN search around each point
    val resultCollection = new DefaultFeatureCollection
    val searchFeatureIterator = new RichSimpleFeatureIterator(inputFeatures.features())

    // for each entry in the inputFeatures collection:
    while (searchFeatureIterator.hasNext) {
      val aFeatureForSearch = searchFeatureIterator.next()
      aFeatureForSearch.getDefaultGeometry match {
        case geo: Point =>
          val knnResults = KNNQuery.runNewKNNQuery(source, query, numDesired, estimatedDistance, maxSearchDistance, aFeatureForSearch)
          // extract the SimpleFeatures and convert to a Collection. Ordering will not be preserved.
          val sfList = knnResults.map {_.sf}.asJavaCollection
          resultCollection.addAll(sfList)
        case _ => log.warn("K Nearest Neighbor Search not implemented for non-point geometries, skipping this Feature")
      }
    }
    resultCollection
  }
}
case class KNNResult(results: SimpleFeatureCollection) extends AbstractCalcResult
