/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.geotools.data.DataUtilities
import org.geotools.filter.spatial.BBOXImpl
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.filter.visitor.IdDetectingFilterVisitor
import org.locationtech.geomesa.utils.geohash.GeohashUtils._
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.spatial._
import org.opengis.filter.temporal.{After, Before, During, TEquals}
import org.opengis.temporal.Period

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

object FilterHelper extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.GeometryUtils.{geoFactory => gf}
  import org.locationtech.geomesa.utils.geotools.{EmptyGeometry, WholeWorldPolygon}

  val MinDateTime = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.UTC)
  val MaxDateTime = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.UTC)

  val DisjointGeometries: Seq[Geometry] = Seq(EmptyGeometry)
  val DisjointInterval: Seq[(DateTime, DateTime)] = Seq((null, null))

  private val SafeGeomString = "gm-safe"

  /**
    * Creates a new filter with valid bounds and attribute
    *
    * @param op spatial op
    * @param sft simple feature type
    * @return valid op
    */
  def visitBinarySpatialOp(op: BinarySpatialOperator, sft: SimpleFeatureType): Filter = {
    val prop = org.locationtech.geomesa.filter.checkOrderUnsafe(op.getExpression1, op.getExpression2)
    val geom = prop.literal.evaluate(null, classOf[Geometry])
    if (geom.getUserData == SafeGeomString) {
      op // we've already visited this geom once
    } else {
      // check for null or empty attribute and replace with default geometry name
      val attribute = Option(prop.name).filterNot(_.isEmpty).getOrElse(if (sft == null) null else sft.getGeomField)
      // copy the geometry so we don't modify the original
      val geomCopy = gf.createGeometry(geom)
      // trim to world boundaries
      val trimmedGeom = geomCopy.intersection(WholeWorldPolygon)
      // add waypoints if needed so that IDL is handled correctly
      val geomWithWayPoints = if (op.isInstanceOf[BBOX]) addWayPointsToBBOX(trimmedGeom) else trimmedGeom
      val safeGeometry = tryGetIdlSafeGeom(geomWithWayPoints)
      // mark it as being visited
      safeGeometry.setUserData(SafeGeomString)
      recreateAsIdlSafeFilter(op, attribute, safeGeometry, prop.flipped)
    }
  }

  /**
    * Creates a new filter with valid bounds and attributes. Distance will be converted into degrees.
    * Note: units will still refer to 'meters', but that is due to ECQL issues
    *
    * @param op dwithin
    * @param sft simple feature type
    * @return valid dwithin
    */
  def visitDwithin(op: DWithin, sft: SimpleFeatureType): Filter = {
    val prop = org.locationtech.geomesa.filter.checkOrderUnsafe(op.getExpression1, op.getExpression2)
    val geom = prop.literal.evaluate(null, classOf[Geometry])
    if (geom.getUserData == SafeGeomString) {
      op // we've already visited this geom once
    } else {
      val units = Option(op.getDistanceUnits).map(_.trim).filter(_.nonEmpty).map(_.toLowerCase).getOrElse("meters")
      val multiplier = units match {
        case "meters"         => 1.0
        case "kilometers"     => 1000.0
        case "feet"           => 0.3048
        case "statute miles"  => 1609.347
        case "nautical miles" => 1852.0
        case _                => 1.0 // not part of ECQL spec...
      }
      val distanceMeters = op.getDistance * multiplier
      val distanceDegrees = GeometryUtils.distanceDegrees(geom, distanceMeters)

      // check for null or empty attribute and replace with default geometry name
      val attribute = Option(prop.name).filterNot(_.isEmpty).getOrElse(if (sft == null) null else sft.getGeomField)
      // copy the geometry so we don't modify the original
      val geomCopy = gf.createGeometry(geom)
      // trim to world boundaries
      val trimmedGeom = geomCopy.intersection(WholeWorldPolygon)
      val safeGeometry = tryGetIdlSafeGeom(trimmedGeom)
      // mark it as being visited
      safeGeometry.setUserData(SafeGeomString)
      recreateAsIdlSafeFilter(op, attribute, safeGeometry, prop.flipped, distanceDegrees)
    }
  }

  private def tryGetIdlSafeGeom(geom: Geometry): Geometry = getInternationalDateLineSafeGeometry(geom) match {
    case Success(g) => g
    case Failure(e) => logger.warn(s"Error splitting geometry on IDL for $geom", e); geom
  }

  private def recreateAsIdlSafeFilter(op: BinarySpatialOperator,
                                      property: String,
                                      geom: Geometry,
                                      flipped: Boolean,
                                      args: Any = null): Filter = {
    geom match {
      case g: GeometryCollection =>
        // geometry collections are OR'd together
        val asList = getGeometryListOf(g)
        asList.foreach(_.setUserData(geom.getUserData))
        ff.or(asList.map(recreateFilter(op, property, _, flipped, args)))
      case _ => recreateFilter(op, property, geom, flipped, args)
    }
  }

  private def recreateFilter(op: BinarySpatialOperator,
                             property: String,
                             geom: Geometry,
                             flipped: Boolean,
                             args: Any): Filter = {
    val (e1, e2) = if (flipped) (ff.literal(geom), ff.property(property)) else (ff.property(property), ff.literal(geom))
    op match {
      case op: Within     => ff.within(e1, e2)
      case op: Intersects => ff.intersects(e1, e2)
      case op: Overlaps   => ff.overlaps(e1, e2)
      // note: The ECQL spec doesn't allow for us to put the measurement
      // in "degrees", but that's how this filter will be used.
      case op: DWithin    => ff.dwithin(e1, e2, args.asInstanceOf[Double], "meters")
      // use the direct constructor so that we preserve our geom user data
      case op: BBOX       => new BBOXImpl(e1, e2)
    }
  }

  def isFilterWholeWorld(f: Filter): Boolean = f match {
      case op: BBOX       => isOperationGeomWholeWorld(op)
      case op: Within     => isOperationGeomWholeWorld(op)
      case op: Intersects => isOperationGeomWholeWorld(op)
      case op: Overlaps   => isOperationGeomWholeWorld(op)
      case _ => false
    }

  private def isOperationGeomWholeWorld[Op <: BinarySpatialOperator](op: Op): Boolean = {
    val prop = checkOrder(op.getExpression1, op.getExpression2)
    prop.map(_.literal.evaluate(null, classOf[Geometry])).exists(isWholeWorld)
  }

  def isWholeWorld[G <: Geometry](g: G): Boolean = g != null && g.union.covers(WholeWorldPolygon)

  def getGeometryListOf(inMP: Geometry): Seq[Geometry] =
    for( i <- 0 until inMP.getNumGeometries ) yield inMP.getGeometryN(i)

  def addWayPointsToBBOX(g: Geometry): Geometry = {
    val gf = g.getFactory
    val geomArray = g.getCoordinates
    val correctedGeom = GeometryUtils.addWayPoints(geomArray).toArray
    if (geomArray.length == correctedGeom.length) g else gf.createPolygon(correctedGeom)
  }

  /**
    * Extracts geometries from a filter into a sequence of OR'd geometries
    *
    * @param filter filter to evaluate
    * @param attribute attribute to consider
    * @param intersect intersect AND'd geometries or return them all
    * @return geometry bounds from spatial filters
    */
  def extractGeometries(filter: Filter, attribute: String, intersect: Boolean = false): Seq[Geometry] =
    extractUnclippedGeometries(filter, attribute, intersect).map(_.intersection(WholeWorldPolygon))

  /**
    * Extract geometries from a filter without validating boundaries.
    *
    * @param filter filter to evaluate
    * @param attribute attribute to consider
    * @param intersect intersect AND'd geometries or return them all
    * @return geometry bounds from spatial filters
    */
  private def extractUnclippedGeometries(filter: Filter, attribute: String, intersect: Boolean): Seq[Geometry] = {
    filter match {
      case o: Or  => o.getChildren.flatMap(extractUnclippedGeometries(_, attribute, intersect))

      case a: And =>
        val all = a.getChildren.map(extractUnclippedGeometries(_, attribute, intersect)).filter(_.nonEmpty)
        if (all.isEmpty) {
          Seq.empty
        } else if (intersect) {
          all.reduceLeft[Seq[Geometry]] { case (g1, g2) =>
            if (g1.isEmpty) { DisjointGeometries } else {
              val gc1 = if (g1.length == 1) g1.head else new GeometryCollection(g1.toArray, g1.head.getFactory)
              val gc2 = if (g2.length == 1) g2.head else new GeometryCollection(g2.toArray, g2.head.getFactory)
              gc1.intersection(gc2) match {
                case g if g.isEmpty => DisjointGeometries
                case g: GeometryCollection => (0 until g.getNumGeometries).map(g.getGeometryN)
                case g => Seq(g)
              }
            }
          }
        } else {
          all.flatten
        }

      // Note: although not technically required, all known spatial predicates are also binary spatial operators
      case f: BinarySpatialOperator if isSpatialFilter(f) =>
        val geometry = for {
          prop <- checkOrder(f.getExpression1, f.getExpression2)
          if prop.name == null || prop.name == attribute
          geom <- Option(prop.literal.evaluate(null, classOf[Geometry]))
        } yield {
          val buffered = filter match {
            // note: the dwithin should have already between rewritten
            case dwithin: DWithin => geom.buffer(dwithin.getDistance)
            case bbox: BBOX =>
              val geomCopy = gf.createGeometry(geom)
              val trimmedGeom = geomCopy.intersection(WholeWorldPolygon)
              addWayPointsToBBOX(trimmedGeom)
            case _ => geom
          }
          tryGetIdlSafeGeom(buffered)
        }
        geometry match {
          case Some(g: GeometryCollection) => (0 until g.getNumGeometries).map(g.getGeometryN)
          case Some(g) => Seq(g)
          case None    => Seq.empty
        }

      case _ => Seq.empty
    }
  }

  /**
    * Extracts intervals from a filter. Intervals will be merged where possible - the resulting sequence
    * is considered to be a union (i.e. OR)
    *
    * @param filter filter to evaluate
    * @param attribute attribute to consider
    * @return a sequence of intervals, if any. disjoint intervals will result in Seq((null, null))
    */
  def extractIntervals(filter: Filter,
                       attribute: String,
                       handleExclusiveBounds: Boolean = false): Seq[(DateTime, DateTime)] = {

    def roundSecondsUp(dt: DateTime): DateTime = dt.plusSeconds(1).withMillisOfSecond(0)
    def roundSecondsDown(dt: DateTime): DateTime = {
      val millis = dt.getMillisOfSecond
      if (millis == 0) dt.minusSeconds(1) else dt.withMillisOfSecond(0)
    }

    extractAttributeBounds(filter, attribute, classOf[Date]).toSeq.flatMap { fb =>
      if (fb.bounds.isEmpty) { DisjointInterval } else {
        fb.bounds.map { bounds =>
          def roundLo(dt: DateTime) = if (handleExclusiveBounds && !bounds.inclusive) roundSecondsUp(dt) else dt
          def roundUp(dt: DateTime) = if (handleExclusiveBounds && !bounds.inclusive) roundSecondsDown(dt) else dt

          val lower = bounds.lower.map(new DateTime(_, DateTimeZone.UTC)).map(roundLo).getOrElse(MinDateTime)
          val upper = bounds.upper.map(new DateTime(_, DateTimeZone.UTC)).map(roundUp).getOrElse(MaxDateTime)

          (lower, upper)
        }
      }
    }
  }

  /**
    * Extracts bounds from filters that pertain to a given attribute. Bounds will be merged where
    * possible.
    *
    * @param filter filter to evaluate
    * @param attribute attribute name to consider
    * @return a sequence of bounds, if any
    */
  def extractAttributeBounds[T](filter: Filter, attribute: String, binding: Class[T]): Option[FilterBounds[T]] = {
    filter match {
      case a: And =>
        val all = a.getChildren.flatMap(extractAttributeBounds(_, attribute, binding))
        all.reduceLeftOption[FilterBounds[T]] { case (left, right) => left.and(right) }

      case o: Or =>
        val all = o.getChildren.flatMap(extractAttributeBounds(_, attribute, binding))
        all.reduceLeftOption[FilterBounds[T]] { case (left, right) => left.or(right) }

      case f: PropertyIsEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val bounds = Bounds(Some(lit), Some(lit), inclusive = true)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case f: PropertyIsBetween =>
        try {
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (prop != attribute) { None } else {
            val lower = f.getLowerBoundary.evaluate(null, binding)
            val upper = f.getUpperBoundary.evaluate(null, binding)
            // note that between is inclusive
            val bounds = Bounds(Option(lower), Option(upper), inclusive = true)
            Some(FilterBounds(prop, Seq(bounds)))
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Unable to extract bounds from filter '${filterToString(f)}'", e)
            None
        }

      case f: During if classOf[Date].isAssignableFrom(binding) =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, classOf[Period])).map { p =>
            val lower = p.getBeginning.getPosition.getDate.asInstanceOf[T]
            val upper = p.getEnding.getPosition.getDate.asInstanceOf[T]
            // note that during is exclusive
            val bounds = Bounds(Option(lower), Option(upper), inclusive = false)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case f: PropertyIsGreaterThan =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (None, Some(lit)) else (Some(lit), None)
            val bounds = Bounds(lower, upper, inclusive = false)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case f: PropertyIsGreaterThanOrEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (None, Some(lit)) else (Some(lit), None)
            val bounds = Bounds(lower, upper, inclusive = true)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case f: PropertyIsLessThan =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (Some(lit), None) else (None, Some(lit))
            val bounds = Bounds(lower, upper, inclusive = false)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case f: PropertyIsLessThanOrEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (Some(lit), None) else (None, Some(lit))
            val bounds = Bounds(lower, upper, inclusive = true)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case f: Before =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (Option(lit), None) else (None, Option(lit))
            // note that before is exclusive
            val bounds = Bounds(lower, upper, inclusive = false)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case f: After =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (None, Option(lit)) else (Option(lit), None)
            // note that after is exclusive
            val bounds = Bounds(lower, upper, inclusive = false)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case f: PropertyIsLike if binding == classOf[String] =>
        try {
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (prop != attribute) { None } else {
            // Remove the trailing wildcard and create a range prefix
            val literal = f.getLiteral
            val lower = if (literal.endsWith(MULTICHAR_WILDCARD)) {
              literal.substring(0, literal.length - MULTICHAR_WILDCARD.length)
            } else {
              literal
            }
            val upper = Some(lower + WILDCARD_SUFFIX).asInstanceOf[Some[T]]
            val bounds = Bounds(Some(lower.asInstanceOf[T]), upper, inclusive = true)
            Some(FilterBounds(prop, Seq(bounds)))
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Unable to extract bounds from filter '${filterToString(f)}'", e)
            None
        }

      case f: Not if f.getFilter.isInstanceOf[PropertyIsNull] =>
        try {
          val isNull = f.getFilter.asInstanceOf[PropertyIsNull]
          val prop = isNull.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (prop != attribute) { None } else {
            val bounds = Bounds[T](None, None, inclusive = true)
            Some(FilterBounds(prop, Seq(bounds)))
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Unable to extract bounds from filter '${filterToString(f)}'", e)
            None
        }

      case f: Not =>
        // we extract the sub-filter bounds, then invert them
        extractAttributeBounds(f.getFilter, attribute, binding).flatMap { inverted =>
          // NOT(A OR B) turns into NOT(A) AND NOT(B)
          val uninverted = inverted.bounds.map { bound =>
            // NOT the single bound
            val not = bound.bounds match {
              case (None, None) => Seq.empty
              case (Some(lo), None) => Seq(Bounds(None, Some(lo), !bound.inclusive))
              case (None, Some(hi)) => Seq(Bounds(Some(hi), None, !bound.inclusive))
              case (Some(lo), Some(hi)) =>
                Seq(Bounds(None, Some(lo), !bound.inclusive), Bounds(Some(hi), None, !bound.inclusive))
            }
            FilterBounds(attribute, not)
          }
          // AND together
          uninverted.reduceLeftOption[FilterBounds[T]] { case (left, right) => left.and(right) }
        }

      case f: TEquals =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val bounds = Bounds(Some(lit), Some(lit), inclusive = true)
            FilterBounds(prop.name, Seq(bounds))
          }
        }

      case _ => None
    }
  }

  def propertyNames(filter: Filter, sft: SimpleFeatureType): Seq[String] =
    DataUtilities.propertyNames(filter, sft).map(_.getPropertyName).toSeq.sorted

  def hasIdFilter(filter: Filter): Boolean =
    filter.accept(new IdDetectingFilterVisitor, false).asInstanceOf[Boolean]

  def filterListAsAnd(filters: Seq[Filter]): Option[Filter] = andOption(filters)
}

