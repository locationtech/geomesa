/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.operation.distance.DistanceOp
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator, SimpleFeatureSource}
import org.geotools.feature.collection.DecoratingSimpleFeatureCollection
import org.geotools.feature.visitor._
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.geotools.referencing.GeodeticCalculator
import org.geotools.util.{Converters, NullProgressListener}
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureCollection
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.filter.{ff, orFilters}
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.Expression
import org.opengis.filter.spatial.DWithin
import org.opengis.filter.{Filter, Or}

@DescribeProcess(
  title = "Route Search",
  description = "Performs a search based on a route"
)
class RouteSearchProcess extends VectorProcess with BinProcessOutput with LazyLogging {

  /**
    * Finds features around a route that are heading along the route and not just crossing over it
    *
    * @param features input features to query
    * @param routes routes to match
    * @param bufferSize buffer around the routes to search, in meters
    * @param headingThreshold threshold to match the heading of the route, in degrees
    * @param routeGeomField geometry attribute in the route features that contains the route, optional
    *                       will use default geometry if not provided
    * @param geomField geometry attribute in input features to match with the route, optional
    *                  will use default geometry if not provided
    * @param bidirectional consider the direction of the route or just the path of the route
    * @param headingField heading attribute in input features, required unless input geometries are linestrings
    * @param bins encode the result in bin format
    * @param binGeom geometry attribute to encode in the bin format, optional
    *                will use default geometry if not provided
    * @param binDtg date attribute to encode in the bin format, optional
    *               will use default date if not provided
    * @param binTrackId track id attribute to encode in the bin format, optional
    *                   will use feature id if not provided
    * @param binLabel label attribute to encode in the bin format, optional
    * @return
    */
  @DescribeResult(description = "Output feature collection")
  def execute(
              @DescribeParameter(name = "features", description = "Input feature collection to query ")
              features: SimpleFeatureCollection,
              @DescribeParameter(name = "routes", description = "Routes to search along. Features must have a geometry of LineString")
              routes: SimpleFeatureCollection,
              @DescribeParameter(name = "bufferSize", description = "Buffer size (in meters) to search around the route")
              bufferSize: java.lang.Double,
              @DescribeParameter(name = "headingThreshold", description = "Threshold for comparing headings, in degrees")
              headingThreshold: java.lang.Double,
              @DescribeParameter(name = "routeGeomField", description = "Attribute that will be examined for routes to match. Must be a LineString", min = 0)
              routeGeomField: String,
              @DescribeParameter(name = "geomField", description = "Attribute that will be examined for route matching. Must be a LineString", min = 0)
              geomField: String,
              @DescribeParameter(name = "bidirectional", description = "Match the route direction or match just the route path", min = 0)
              bidirectional: java.lang.Boolean,
              @DescribeParameter(name = "headingField", description = "Attribute that will be examined for heading in the input features. If not provided, input features geometries must be LineStrings", min = 0)
              headingField: String,
              @DescribeParameter(name = "bins", description = "Return BIN records instead of regular records", min = 0)
              bins: java.lang.Boolean,
              @DescribeParameter(name = "binGeom", description = "Geometry field to use for BIN records", min = 0)
              binGeom: String,
              @DescribeParameter(name = "binDtg", description = "Date field to use for BIN records", min = 0)
              binDtg: String,
              @DescribeParameter(name = "binTrackId", description = "Track field to use for BIN records", min = 0)
              binTrackId: String,
              @DescribeParameter(name = "binLabel", description = "Label field to use for BIN records", min = 0)
              binLabel: String
             ): SimpleFeatureCollection = {

    logger.debug(s"Route searching on collection type ${features.getClass.getName}")

    val bi = Option(bidirectional).getOrElse(java.lang.Boolean.FALSE)

    if (!features.isInstanceOf[AccumuloFeatureCollection]) {
      logger.warn(s"The provided feature collection type may not support distributed route search: ${features.getClass.getName}")
    }

    val sft = features.getSchema

    // pull out attributes and validate inputs

    val (geomAttribute, isPoints) = {
      val name = Option(geomField).getOrElse(sft.getGeometryDescriptor.getLocalName)
      val descriptor = sft.getDescriptor(name)
      if (descriptor == null) {
        throw new IllegalArgumentException(s"Geometry field '$name' does not exist in input feature collection")
      }
      val binding = descriptor.getType.getBinding
      val isPoints = classOf[Point].isAssignableFrom(binding)
      if (!isPoints && !classOf[LineString].isAssignableFrom(binding)) {
        throw new IllegalArgumentException(s"Geometry field '$name' must be a Point or LineString")
      }
      (name, isPoints)
    }

    if (headingField == null && isPoints) {
      throw new IllegalArgumentException("Heading must be specified unless input feature collection geometry is a LineString")
    } else if (headingField != null && sft.indexOf(headingField) == -1) {
      throw new IllegalArgumentException(s"Heading field '$headingField' does not exist in input feature collection")
    }

    // extract the route geometries
    val routeGeoms = {
      val sft = routes.getSchema
      val name = Option(routeGeomField).getOrElse(sft.getGeometryDescriptor.getLocalName)
      val index = sft.indexOf(name)
      if (index == -1) {
        throw new IllegalArgumentException(s"Geometry field '$name' does not exist in route feature collection")
      }
      if (!classOf[LineString].isAssignableFrom(sft.getDescriptor(index).getType.getBinding)) {
        throw new IllegalArgumentException(s"Route geometry field '$name' must be a LineString")
      }
      SelfClosingIterator(routes).map(_.getAttribute(index).asInstanceOf[LineString]).toSeq
    }

    val visitor = new RouteVisitor(routeGeoms, bufferSize, headingThreshold, bi, geomAttribute, isPoints, Option(headingField))

    features.accepts(visitor, new NullProgressListener)
    val results = visitor.getResult.results

    configureOutput(features.getSchema, results, bins, binGeom, binDtg, binTrackId, binLabel)

    results
  }
}

class RouteVisitor(routes: Seq[LineString],
                   routeBuffer: Double,
                   threshold: Double,
                   bidirectional: Boolean,
                   geomAttribute: String,
                   isPoints: Boolean,
                   headingAttribute: Option[String]) extends FeatureCalc with FeatureAttributeVisitor with LazyLogging {

  private var resultCalc: RouteResult = _

  private val routeFilter =
    orFilters(routes.map(ls => ff.dwithin(ff.property(geomAttribute), ff.literal(ls), routeBuffer, "meters")))

  // for manual check, rewrite the filter to handle meters
  // normally handled in our query planner, but we are going to use the filter directly here
  private lazy val manualRouteFilter = routeFilter match {
    case f: DWithin => new QueryPlanFilterVisitor(null).visit(f, null).asInstanceOf[Filter]
    case f: Or      => new QueryPlanFilterVisitor(null).visit(f, null).asInstanceOf[Filter]
  }

  // for collecting results manually
  private var manualCollection: ListFeatureCollection = _

  override def getResult: RouteResult = resultCalc

  // manually called for non-accumulo feature collections
  override def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]

    if (manualCollection == null) {
      manualCollection = new ListFeatureCollection(sf.getFeatureType)
      resultCalc = RouteResult(matchRoutes(manualCollection))
    }

    if (manualRouteFilter.evaluate(sf)) {
      manualCollection.add(sf)
    }
  }

  // allows us to accept visitors from retyping feature collections
  override def getExpressions: java.util.List[Expression] = {
    import scala.collection.JavaConversions._
    headingAttribute.map(ff.property).toSeq :+ ff.property(geomAttribute).asInstanceOf[Expression]
  }

  /**
    * Optimized method to run distributed query. Sets the result, available from `getResult`
    *
    * @param source simple feature source
    * @param query may contain additional filters to apply
    */
  def routeSearch(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Visiting source type: ${source.getClass.getName}")
    val result = if (routes.isEmpty) { source.getFeatures(Filter.EXCLUDE) } else {
      import org.locationtech.geomesa.filter._
      val filter = if (query == null || query.getFilter == Filter.INCLUDE) { routeFilter } else {
        andFilters(Seq(query.getFilter, routeFilter))
      }
      matchRoutes(source.getFeatures(filter))
    }
    resultCalc = RouteResult(result)
  }

  /**
    * Filters a feature collection by comparing routes
    *
    * @param input input feature collection
    * @return
    */
  private def matchRoutes(input: SimpleFeatureCollection): SimpleFeatureCollection = {
    val sft = input.getSchema
    val geomIndex = sft.indexOf(geomAttribute)
    val headingIndex = headingAttribute.map(sft.indexOf)

    logger.debug(s"Searching routes: ${routes.map(WKTUtils.write).mkString(", ")}")
    logger.debug(s"Buffer (meters): $routeBuffer")
    logger.debug(s"Geometry attribute: $geomAttribute")
    logger.debug(s"Heading attribute: ${headingAttribute.getOrElse("none")}")

    RouteVisitor.matchRoutes(input, routes, geomIndex, isPoints, headingIndex, threshold, bidirectional)
  }
}

object RouteVisitor {

  /**
    * Match features to routes.
    *
    * @param input input features
    * @param routes routes to match against
    * @param geomIndex geometry attribute index of the input features
    * @param isPoints are the input feature geometries points (or linestrings)
    * @param headingIndex attribute index of the heading in the input features, not required if they are linestrings
    * @param threshold threshold to consider when matching route heading, in degrees
    * @param bidirectional match the route direction or just the path
    * @return
    */
  def matchRoutes(input: SimpleFeatureCollection,
                  routes: Seq[LineString],
                  geomIndex: Int,
                  isPoints: Boolean,
                  headingIndex: Option[Int],
                  threshold: Double,
                  bidirectional: Boolean): SimpleFeatureCollection = {

    // just in case the features are operated on in parallel...
    val calculator = new ThreadLocal[GeodeticCalculator] {
      override def initialValue(): GeodeticCalculator = new GeodeticCalculator
    }
    val headingCache = {
      import scala.collection.JavaConverters._
      new ConcurrentHashMap[(LineString, Int), Double].asScala
    }

    // gets the heading for an input feature
    val getFeatureHeading: (SimpleFeature) => Double = headingIndex match {
      case Some(index) => (sf) => Converters.convert(sf.getAttribute(index), classOf[Double])
      case None =>
      (sf) => {
        val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
        // use the last two points in the line to find it's current heading - we don't care about history
        getRouteHeading(geom, geom.getNumPoints - 2, calculator.get)
      }
    }

    // gets the point from a geometry that we want to match against routes
    val getComparisonPoint: (Geometry) => Point = if (isPoints) {
      (g) => g.asInstanceOf[Point]
    } else {
      // match the most recent point
      (g) => g.asInstanceOf[LineString].getEndPoint
    }

    // matches a feature against the routes
    def matchRoute(sf: SimpleFeature): Boolean = {
      val geom = sf.getAttribute(geomIndex).asInstanceOf[Geometry]
      val (route, closestLocation) = getClosestRoute(getComparisonPoint(geom), routes)
      val routeHeading = headingCache.getOrElseUpdate((route, closestLocation),
        getRouteHeading(route, closestLocation, calculator.get))
      val featureHeading = getFeatureHeading(sf)

      // compare the headings
      var diff = math.abs(routeHeading - featureHeading)
      // compass problem - correct for headings on either side of 0/360
      if (diff > 180.0) {
        diff = math.abs(diff - 360.0)
      }
      diff <= threshold || (bidirectional && math.abs(diff - 180.0) <= threshold)
    }

    // delegate feature collection that filters the results based on route matching
    new DecoratingSimpleFeatureCollection(input) {
      override def features(): SimpleFeatureIterator = new SimpleFeatureIterator() {
        private val delegate = CloseableIterator(input.features()).filter(matchRoute)
        override def next(): SimpleFeature = delegate.next()
        override def hasNext: Boolean = delegate.hasNext
        override def close(): Unit = delegate.close()
      }
    }
  }

  /**
    * Gets the closest route to the input point
    *
    * @param geom input point
    * @param routes routes to check
    * @return route and index of start of line segment in the route that is closest to the point
    */
  private def getClosestRoute(geom: Point, routes: Seq[LineString]): (LineString, Int) = {
    def closestPoint(ls: LineString): (Double, LineString, Int) = {
      val op = new DistanceOp(ls, geom)
      (op.distance(), ls, op.nearestLocations()(0).getSegmentIndex)
    }
    val (_, route, location) = routes.map(closestPoint).minBy(_._1)
    (route, location)
  }

  /**
    * Gets the heading of a route at a particular line segment
    *
    * @param route route
    * @param index index of the start of the line segment we want to consider
    * @param calculator geodetic calculator instance
    * @return heading between 0-360
    */
  private def getRouteHeading(route: LineString, index: Int, calculator: GeodeticCalculator): Double = {
    val coords = route.getCoordinates
    // the closest point falls somewhere along this line segment - calculate the heading for the segment
    val segment0 = coords(index)
    val segment1 = coords(index + 1)
    calculator.setStartingGeographicPoint(segment0.x, segment0.y)
    calculator.setDestinationGeographicPoint(segment1.x, segment1.y)
    val azimuth = calculator.getAzimuth
    // azimuth is between -180 and +180, where 0 corresponds to north - convert to degrees 0-360
    if (azimuth < 0.0) {
      360.0 + azimuth
    } else {
      azimuth
    }
  }
}

case class RouteResult(results: SimpleFeatureCollection) extends AbstractCalcResult
