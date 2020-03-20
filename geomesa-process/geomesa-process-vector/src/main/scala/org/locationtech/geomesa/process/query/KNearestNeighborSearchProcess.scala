/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter.ff
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess.KNNVisitor
import org.locationtech.geomesa.process.{FeatureResult, GeoMesaProcess}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geometry.DistanceCalculator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, GeometryUtils}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.opengis.filter.expression.PropertyName

@DescribeProcess(
  title = "Geomesa-enabled K Nearest Neighbor Search",
  description = "Performs a K-nearest-neighbor search on a feature collection using a second feature collection as input"
)
class KNearestNeighborSearchProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "inputFeatures",
                 description = "Feature collection that defines the points to search")
               inputFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "dataFeatures",
                 description = "Feature collection to query for nearest neighbors")
               dataFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "numDesired",
                 description = "k, the number of nearest neighbors to return")
               numDesired: java.lang.Integer,

               @DescribeParameter(
                 name = "estimatedDistance",
                 description = "Estimate of the distance in meters for the k-th nearest neighbor, used for the initial query window")
               estimatedDistance: java.lang.Double,

               @DescribeParameter(
                 name = "maxSearchDistance",
                 description = "Maximum search distance in meters, used to prevent runaway queries of the entire data set")
               maxSearchDistance: java.lang.Double
               ): SimpleFeatureCollection = {

    logger.debug(
      s"Running KNN query for ${dataFeatures.getClass.getName} with k = $numDesired, " +
        s"initial distance = $estimatedDistance, max distance = $maxSearchDistance")

    val visitor = new KNNVisitor(inputFeatures, numDesired, estimatedDistance, maxSearchDistance)
    GeoMesaFeatureCollection.visit(dataFeatures, visitor)
    visitor.getResult.results
  }
}

object KNearestNeighborSearchProcess {

  private val WholeWorldEnvelope = Envelope(-180d, 180d, -90d, 90d)

  /**
    * Main visitor class for the KNN search process
    *
    * @param query query features - geometries will be used as the inputs
    * @param k number of neighbors to find
    * @param start initial distance to search
    * @param threshold max distance to search
    */
  class KNNVisitor(query: SimpleFeatureCollection, k: Int, start: Double, threshold: Double)
      extends GeoMesaProcessVisitor with LazyLogging {

    import org.locationtech.geomesa.filter.ff
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    private lazy val queries: Seq[Point] = SelfClosingIterator(query.features()).toList.flatMap { f =>
      f.getDefaultGeometry match {
        case p: Point => Some(p)
        case g => logger.warn(s"KNN query not implemented for non-point geometries, skipping this feature: $g"); None
      }
    }

    private lazy val results = Seq.fill(queries.length)(Array.ofDim[FeatureWithDistance](k))
    private lazy val calculators = queries.zip(results).map { case (p, r) => new KnnCalculator(p, k, threshold, r) }

    private var result: FeatureResult = _

    // called for non-optimized visits
    override def visit(feature: Feature): Unit = calculators.foreach(_.visit(feature.asInstanceOf[SimpleFeature]))

    override def getResult: FeatureResult = {
      if (result == null) {
        val collection = new DefaultFeatureCollection()
        results.foreach(_.foreach(r => if (r != null) { collection.add(r.sf) }))
        result = FeatureResult(collection)
      }
      result
    }

    override def execute(source: SimpleFeatureSource, query: Query): Unit = {
      logger.debug(s"Running Geomesa KNN process on source type ${source.getClass.getName}")

      // create a new feature collection to hold the results of the KNN search around each point
      val collection = new DefaultFeatureCollection()
      val geom = ff.property(source.getSchema.getGeomField)

      // for each entry in the inputFeatures collection:
      queries.par.foreach { p =>
        // tracks our nearest neighbors
        val results = Array.ofDim[FeatureWithDistance](k)
        // tracks features are in our search envelope but that aren't within our current search distance
        // we use a java linked list as scala doesn't have anything with 'iterate and remove' functionality
        val overflow = new java.util.LinkedList[FeatureWithDistance]()

        // calculator for expanding window queries
        val window = new KnnWindow(query, geom, p, k, start, threshold)
        var found = 0 // tracks the number of neighbors we've found so far

        val initial = window.next(None)
        logger.trace(s"Current query window:\n  ${window.window.debug.mkString("\n  ")}")
        logger.trace(s"Current filter: ${ECQL.toCQL(initial.getFilter)}")

        // run our first query against the estimated distance
        WithClose(source.getFeatures(initial).features()) { iter =>
          // note: the calculator will populate our results array
          val knn = new KnnCalculator(p, k, window.radius, results)
          while (iter.hasNext) {
            val sf = iter.next
            // features that aren't within our query distance are added to the overflow for the next iteration
            knn.visit(sf).foreach(d => overflow.add(FeatureWithDistance(sf, d)))
          }
          found = knn.size
        }

        var iteration = 1 // tracks the number of queries we've run

        // if we haven't found k features, start expanding the search distance
        while (found < k && window.hasNext) {
          val last = window.radius
          // calculate the next query to search, based on how many we've found so far
          val next = window.next(Some(found))
          logger.debug(
            s"Expanding search at (${p.getX} ${p.getY}) from $last to ${window.radius} meters " +
                s"based on finding $found/$k neighbors")
          logger.trace(s"Current query window:\n  ${window.window.debug.mkString("\n  ")}")
          logger.trace(s"Current filter: ${ECQL.toCQL(next.getFilter)}")

          // calculator for our new distance, initialized with the neighbors we've already found
          val knn = new KnnCalculator(p, k, window.radius, results, found)
          // re-visit any features that were outside our distance but inside our last envelope
          val iter = overflow.iterator()
          while (iter.hasNext) {
            val FeatureWithDistance(sf, d) = iter.next()
            if (d <= window.radius) {
              knn.visit(sf, d)
              iter.remove()
            }
          }

          // run the new query
          WithClose(source.getFeatures(next).features()) { iter =>
            while (iter.hasNext) {
              val sf = iter.next
              // features that aren't within our query distance are added to the overflow for the next iteration
              knn.visit(sf).foreach(d => overflow.add(FeatureWithDistance(sf, d)))
            }
            found = knn.size
          }

          iteration += 1
        }

        logger.debug(
          s"Found $found/$k neighbors at (${p.getX} ${p.getY}) after $iteration iteration(s) with final search " +
              s"distance of ${window.radius} (initial $start, max $threshold)")
        logger.trace(results.take(found).map(_.sf).mkString("; "))

        collection.synchronized {
          var i = 0
          while (i < found) {
            collection.add(results(i).sf)
            i += 1
          }
        }
      }

      this.result = FeatureResult(collection)
    }
  }

  /**
    * Class for finding window queries around a central point
    *
    * @param base base query
    * @param geom geometry attribute being queried
    * @param p central query point
    * @param k number of features to find
    * @param start initial search distance
    * @param threshold max distance to query
    */
  class KnnWindow(base: Query, geom: PropertyName, p: Point, k: Int, start: Double, threshold: Double) {

    private val calc = new GeodeticCalculator()
    private var distance = start

    // our query envelope - this is purely cartesian and not constrained by world bounds
    private var envelope = QueryEnvelope(Envelope(p, distance, calc), None)

    /**
      * Current search radius, in meters
      *
      * @return
      */
    def radius: Double = distance

    /**
      * Current search envelope
      *
      * @return envelope
      */
    def window: QueryEnvelope = envelope

    /**
      * Whether there are more window queries within the threshold
      *
      * @return
      */
    def hasNext: Boolean = distance < threshold

    /**
      * Get the next query window
      *
      * @param found number of features found so far, if the query has already been run
      * @return
      */
    def next(found: Option[Int]): Query = {
      found.foreach(expand) // expand our window if we've already run a query

      val query = new Query(base)
      query.setHints(new Hints(base.getHints)) // prevent sharing of hints between queries

      // remove the hole from our query envelopes so that we don't scan the same area more than once
      val filter = org.locationtech.geomesa.filter.orFilters(envelope.query.map(_.toFilter(geom)))
      base.getFilter match {
        case null | Filter.INCLUDE => query.setFilter(filter)
        case f => query.setFilter(ff.and(f, filter))
      }

      query
    }

    /**
      * Find the next window to search in an attempt to find the knn
      *
      * @param found number of neighbors found within the previous distance
      */
    private def expand(found: Int): Unit = {
      // expand the window radius. algorithm derived from:
      //    Efficient k Nearest Neighbor Queries on Remote Spatial Databases Using Range Estimation
      //    Danzhou Liu, Ee-Peng Lim, Wee-Keong Ng
      //    Centre for Advanced Information Systems, School of Computer Engineering
      //    Nanyang Technological University, Singapore 639798
      //    {P149571472, aseplim, awkng}@ntu.edu.sg
      //  Available at: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.507.4109&rep=rep1&type=pdf

      // note that this algorithm is fairly conservative - in their testing (using TIGER road data), it
      // generally required 2-3 window expansions after the initial query in order to find the knn

      distance = if (found == 0) { distance * 2 } else {
        val density = found / math.pow(2 * distance, 2)
        math.sqrt(k / (math.Pi * density))
      }
      if (distance > threshold) {
        distance = threshold
      }

      val hole = Some(envelope.envelope)
      envelope = QueryEnvelope(Envelope(p, distance, calc), hole)
    }
  }

  /**
    * Calculator for finding the k-nearest features
    *
    * @param query point to find neighbors for
    * @param k number of neighbors to find
    * @param threshold limit results to within a certain distance in meters
    * @param results array of length k, used to hold results
    * @param i number of features found so far, maxes out at k
    */
  class KnnCalculator(
      query: Point,
      k: Int,
      threshold: Double,
      results: Array[FeatureWithDistance],
      private var i: Int = 0
    ) extends LazyLogging {

    private val calculator = new DistanceCalculator()

    // tracks the index of our current farthest value
    private var fi = if (i < k) { 0 } else { farthest }

    /**
      * Number of features found so far
      *
      * @return
      */
    def size: Int = i

    /**
      * Visit a feature, checking the distance to the query point
      *
      * @param feature feature
      * @return None if the feature is within the threshold, otherwise Some(distance to query point)
      */
    def visit(feature: SimpleFeature): Option[Double] = {
      feature.getDefaultGeometry match {
        case p: Point =>
          val meters = calculator.meters(query, p)
          if (meters > threshold) {
            Some(meters)
          } else {
            visit(feature, meters)
            None
          }

        case g => logger.warn(s"KNN query not implemented for non-point geometries, skipping this feature: $g"); None
      }
    }

    /**
      * Visit a feature with a known distance
      *
      * @param feature feature
      * @param distance distance from the query point, in meters (assumed to be within the threshold)
      */
    def visit(feature: SimpleFeature, distance: Double): Unit = {
      if (i < k) {
        // if we haven't seen k features yet, then by definition we have a nearest neighbor
        results(i) = FeatureWithDistance(feature, distance)
        i += 1
        if (i == k) {
          // if we have k elements, find the farthest feature so that we can easily check it going forward
          fi = farthest
        }
      } else if (distance < results(fi).meters) {
        // we have found a new nearest - replace the farthest, then find the new farthest
        results(fi) = FeatureWithDistance(feature, distance)
        fi = farthest
      }
    }

    /**
      * Gets the array index of the farthest feature. Note: assumes the result array does not contain nulls
      *
      * @return
      */
    private def farthest: Int = {
      var max = results.head.meters
      var result = 0
      var i = 1
      while (i < results.length) {
        if (results(i).meters > max) {
          max = results(i).meters
          result = i
        }
        i += 1
      }
      result
    }
  }

  /**
    * Holder for our ordered features
    *
    * @param sf simple feature
    * @param meters distance in meters from the query point
    */
  case class FeatureWithDistance(sf: SimpleFeature, meters: Double) extends Comparable[FeatureWithDistance] {
    override def compareTo(o: FeatureWithDistance): Int = java.lang.Double.compare(meters, o.meters)
  }

  /**
    * Holder for a query envelope
    *
    * @param envelope query envelope, in cartesian space
    * @param hole hole covering any previously queried space
    */
  case class QueryEnvelope(envelope: Envelope, hole: Option[Envelope]) {

    /**
      * Get the envelopes for a query filter. The envelopes will exclude the hole, if present, to avoid
      * returning duplicate results
      *
      * @return
      */
    def query: Seq[Envelope] = {
      hole match {
        case None => envelope.toWorld
        case Some(h) =>
          val holes = h.toWorld
          envelope.toWorld.flatMap(e => holes.foldLeft(Seq(e)) { case (r, h) => r.flatMap(_.minus(h)) })
      }
    }

    /**
      * Gets a string useful for debugging
      *
      * @return
      */
    def debug: Seq[String] = hole match {
      case None => Seq(envelope.debug)
      case Some(h) =>
        val diff = envelope.minus(h)
        if (diff.isEmpty) { Seq("[empty]") } else { diff.map(_.debug) }
    }
  }

  /**
    * Simple envelope class - unlike the jts envelope, does not swap min/max values on creation
    *
    * @param xmin min x value
    * @param xmax max x value
    * @param ymin min y value
    * @param ymax max y value
    */
  case class Envelope(xmin: Double, xmax: Double, ymin: Double, ymax: Double) {

    require(xmin <= xmax && ymin <= ymax, s"Envelope is inverted: [$xmin, $xmax, $ymin, $ymax]")

    lazy val width: Double = xmax - xmin
    lazy val height: Double = ymax - ymin

    /**
      * Intersection of two envelopes
      *
      * @param other other envelope
      * @return
      */
    def intersection(other: Envelope): Option[Envelope] = {
      val ixmin = math.max(xmin, other.xmin)
      val ixmax = math.min(xmax, other.xmax)
      if (ixmin > ixmax) { None } else {
        val iymin = math.max(ymin, other.ymin)
        val iymax = math.min(ymax, other.ymax)
        if (iymin > iymax) { None } else {
          Some(Envelope(ixmin, ixmax, iymin, iymax))
        }
      }
    }

    /**
      * Do the two envelopes intersect?
      *
      * @param other other envelope
      * @return
      */
    def intersects(other: Envelope): Boolean =
      other.xmin <= xmax && other.xmax >= xmin && other.ymin <= ymax && other.ymax >= ymin

    /**
      * Subtract an envelope from this one.
      *
      * The result will be 0 to 4 new envelopes. The 'top' and 'bottom' envelopes will contain corner
      * intersections, if any, while the 'left' and 'right' envelopes will be trimmed around the top/bottom ones.
      *
      * @param other envelope to subtract
      * @return
      */
    def minus(other: Envelope): Seq[Envelope] = {
      if (!intersects(other)) { Seq(this) } else {
        val builder = Seq.newBuilder[Envelope]
        // conditionally add top envelope, and determine ymax for left/right envelopes
        val sidemax = if (other.ymax >= ymax) { ymax } else {
          builder += copy(ymin = other.ymax)
          other.ymax
        }
        // conditionally add bottom envelope, and determine ymin for left/right envelopes
        val sidemin = if (other.ymin <= ymin) { ymin } else {
          builder += copy(ymax = other.ymin)
          other.ymin
        }
        // conditionally add left/right envelopes
        if (other.xmin > xmin) {
          builder += copy(xmax = other.xmin, ymin = sidemin, ymax = sidemax)
        }
        if (other.xmax < xmax) {
          builder += copy(xmin = other.xmax, ymin = sidemin, ymax = sidemax)
        }
        builder.result
      }
    }

    /**
      * Converts any part of this envelope that extends past [-180,180,-90,90] to handle the anti-meridian and poles.
      *
      * Envelopes that cross the poles end up wrapping the whole longitude. Envelopes that cross the anti-meridian
      * are wrapped to the other side.
      *
      * @return
      */
    def toWorld: Seq[Envelope] = {
      if (ymin < -90d) {
        Seq(Envelope(-180d, 180d, -90, math.min(ymax, 90d)))
      } else if (ymax > 90d) {
        Seq(Envelope(-180d, 180d, math.max(ymin, -90), 90d))
      } else if (width >= 360d) {
        Seq(copy(xmin = -180d, xmax = 180d))
      } else if (xmin < -180d) {
        intersection(WholeWorldEnvelope) match {
          case None => Seq(copy(xmin = xmin + 360d, xmax = xmax + 360d))
          case Some(trimmed) => Seq(trimmed, copy(xmin = xmin + 360d, xmax = 180d))
        }
      } else if (xmax > 180d) {
        intersection(WholeWorldEnvelope) match {
          case None => Seq(copy(xmin = xmin - 360d, xmax = xmax - 360d))
          case Some(trimmed) => Seq(trimmed, copy(xmin = -180d, xmax = xmax - 360d))
        }
      } else {
        Seq(this)
      }
    }

    /**
      * Create a geotools filter
      *
      * @param geom geometry expression
      * @return
      */
    def toFilter(geom: PropertyName): Filter =
      ff.bbox(geom, new ReferencedEnvelope(xmin, xmax, ymin, ymax, CRS_EPSG_4326))

    /**
      * Convert to a JTS envelope
      *
      * @return
      */
    def toJts: org.locationtech.jts.geom.Envelope = new org.locationtech.jts.geom.Envelope(xmin, xmax, ymin, ymax)

    /**
      * Debug string
      *
      * @return
      */
    def debug: String = s"[$xmin, $xmax, $ymin, $ymax]"
  }

  object Envelope {

    /**
      * Create an envelope by buffering a point to an inscribed circle
      *
      * @param point center point
      * @param buffer buffer distance, in meters
      * @param calc geodetic calculator
      * @return
      */
    def apply(point: Point, buffer: Double, calc: GeodeticCalculator): Envelope = {
      val (east, north) = GeometryUtils.directionalDegrees(point, buffer, calc)
      Envelope(point.getX - east, point.getX + east, point.getY - north, point.getY + north)
    }
  }
}
