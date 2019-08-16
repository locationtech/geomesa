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
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.process.knn.KNearestNeighborSearchProcess.KNNVisitor
import org.locationtech.geomesa.process.{FeatureResult, GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geometry.DistanceCalculator
import org.locationtech.jts.geom.Point
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature

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
                 description = "Estimate of Search Distance in meters for K neighbors - used to set the granularity of the search")
               estimatedDistance: java.lang.Double,

               @DescribeParameter(
                 name = "maxSearchDistance",
                 description = "Maximum search distance in meters - used to prevent runaway queries of the entire table")
               maxSearchDistance: java.lang.Double
               ): SimpleFeatureCollection = {

    logger.debug(s"Running KNN query for ${dataFeatures.getClass.getName}")

    val visitor = new KNNVisitor(inputFeatures, numDesired, estimatedDistance, maxSearchDistance)
    GeoMesaFeatureCollection.visit(dataFeatures, visitor)
    visitor.getResult.results
  }
}

object KNearestNeighborSearchProcess {

  /**
    * Main visitor class for the KNN search process
    *
    * @param query query features - geometries will be used as the inputs
    * @param k number of neighbors to find
    * @param estimatedDistance estimated distance for k features
    * @param maxSearchDistance max distance to search
    */
  class KNNVisitor(query: SimpleFeatureCollection, k: Int, estimatedDistance: Double, maxSearchDistance: Double)
      extends GeoMesaProcessVisitor with LazyLogging {

    private lazy val queries: Seq[Point] = SelfClosingIterator(query.features()).toList.flatMap { f =>
      f.getDefaultGeometry match {
        case p: Point => Some(p)
        case g => logger.warn(s"KNN query not implemented for non-point geometries, skipping this feature: $g"); None
      }
    }

    private lazy val calculator = new KnnCalculator(queries, k)

    private var result: FeatureResult = _

    // called for non-optimized visits
    override def visit(feature: Feature): Unit = calculator.visit(feature.asInstanceOf[SimpleFeature])

    override def getResult: FeatureResult = {
      if (result == null) {
        val features = calculator.result
        if (features.isEmpty) {
          result = FeatureResult(new DefaultFeatureCollection())
        } else {
          result = FeatureResult(new ListFeatureCollection(features.head.getFeatureType, features.asJava))
        }
      }
      result
    }

    /**
      * The KNN-Search interface for the WPS process.
      *
      * Takes as input a Query and SimpleFeatureSource, in addition to
      * inputFeatures which define one or more SimpleFeatures for which to find KNN of each
      */
    override def execute(source: SimpleFeatureSource, query: Query): Unit = {
      logger.debug(s"Running Geomesa KNN process on source type ${source.getClass.getName}")

      // create a new Feature collection to hold the results of the KNN search around each point
      val resultCollection = new DefaultFeatureCollection()

      // for each entry in the inputFeatures collection:
      queries.foreach { p =>
        val knnResults = KNNQuery.runNewKNNQuery(source, query, k, estimatedDistance, maxSearchDistance, p)
        // extract the SimpleFeatures and convert to a Collection. Ordering will not be preserved.
        resultCollection.addAll(knnResults.getK.map(_.sf).asJava)
      }

      result = FeatureResult(resultCollection)
    }
  }

  /**
    * Calculator for finding the k-nearest features
    *
    * @param queries points to find neighbors for
    * @param k number of neighbors to find for each query point
    */
  private class KnnCalculator(queries: Seq[Point], k: Int) extends LazyLogging {

    private val calculator = new DistanceCalculator()

    // an array containing the nearest k features we've seen so far
    private val results = queries.map(q => q -> Array.ofDim[FeatureWithDistance](k))

    private var i = 0 // tracks the number of features we've seen so far, maxes out at k

    private var fi = 0 // tracks the index of our current farthest value

    /**
      * Gets the nearest neighbors seen so far
      *
      * @return
      */
    def result: Seq[SimpleFeature] =
      if (i == k) { results.flatMap(_._2.map(_.sf)) } else { results.flatMap(_._2.take(i).map(_.sf)) }

    /**
      * Visit a feature, checking the distance to each query point
      *
      * @param feature feature
      */
    def visit(feature: SimpleFeature): Unit = {
      feature.getDefaultGeometry match {
        case p: Point =>
          if (i < k) {
            // if we haven't seen k features yet, then by definition we have a nearest neighbor
            results.foreach { case (query, nearest) =>
              val d = calculator.max(query, p)
              nearest(i) = FeatureWithDistance(feature, d.meters, d.degrees)
              if (i == k - 1) {
                // if we have k elements, find the farthest feature so that we can easily check it going forward
                fi = farthest(nearest)
              }
            }
            i += 1
          } else {
            results.foreach { case (query, nearest) =>
              // quickly calculate the distance in degrees using the hypotenuse
              val degrees = math.hypot(math.abs(p.getX - query.getX), math.abs(p.getY - query.getY))
              // if the distance degrees is greater than the max possible degrees for our current farthest
              // feature, then we don't need to do a more precise calculation
              if (degrees <= nearest(fi).degrees) {
                // calculate the meters and do a precise comparison
                val meters = calculator.meters(query, p)
                if (meters < nearest(fi).meters) {
                  // we have found a new nearest - replace the farthest, then find the new farthest
                  nearest(fi) = FeatureWithDistance(feature, meters, calculator.max(query, p, meters))
                  fi = farthest(nearest)
                }
              }
            }
          }

        case g => logger.warn(s"KNN query not implemented for non-point geometries, skipping this feature: $g")
      }
    }
  }

  /**
    * Gets the array index of the farthest feature
    *
    * @param nearest nearest features
    * @return
    */
  private def farthest(nearest: Array[FeatureWithDistance]): Int = {
    var max = nearest.head.meters
    var result = 0
    var i = 1
    while (i < nearest.length) {
      if (nearest(i).meters > max) {
        max = nearest(i).meters
        result = i
      }
      i += 1
    }
    result
  }

  /**
    * Holder for our ordered features
    *
    * @param sf simple feature
    * @param meters distance in meters from the query point
    * @param degrees max possible distance in degrees from the query point
    */
  private case class FeatureWithDistance(sf: SimpleFeature, meters: Double, degrees: Double)
      extends Comparable[FeatureWithDistance] {
    override def compareTo(o: FeatureWithDistance): Int = java.lang.Double.compare(meters, o.meters)
  }
}
