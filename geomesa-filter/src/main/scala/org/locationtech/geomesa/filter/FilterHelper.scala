/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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

import scala.collection.GenTraversableOnce
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

object FilterHelper {

  import org.locationtech.geomesa.utils.geotools.GeometryUtils.{geoFactory => gf}
  import org.locationtech.geomesa.utils.geotools.WholeWorldPolygon

  val MinDateTime = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.UTC)
  val MaxDateTime = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.UTC)

  private val SafeGeomString = "gm-safe"

  // helper shim to let other classes avoid importing FilterHelper.logger
  object FilterHelperLogger extends LazyLogging {
    def log = logger
  }

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
      if (trimmedGeom.isEmpty) {
        Filter.EXCLUDE
      } else {
        // add waypoints if needed so that IDL is handled correctly
        val geomWithWayPoints = if (op.isInstanceOf[BBOX]) addWayPointsToBBOX(trimmedGeom) else trimmedGeom
        val safeGeometry = tryGetIdlSafeGeom(geomWithWayPoints)
        // mark it as being visited
        safeGeometry.setUserData(SafeGeomString)
        recreateAsIdlSafeFilter(op, attribute, safeGeometry, prop.flipped)
      }
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
    case Failure(e) => FilterHelperLogger.log.warn(s"Error splitting geometry on IDL for $geom", e); geom
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
    *                  note if not intersected, 'and/or' distinction will be lost
    * @return geometry bounds from spatial filters
    */
  def extractGeometries(filter: Filter, attribute: String, intersect: Boolean = true): FilterValues[Geometry] =
    extractUnclippedGeometries(filter, attribute, intersect).map(_.intersection(WholeWorldPolygon))

  /**
    * Extract geometries from a filter without validating boundaries.
    *
    * @param filter filter to evaluate
    * @param attribute attribute to consider
    * @param intersect intersect AND'd geometries or return them all
    * @return geometry bounds from spatial filters
    */
  private def extractUnclippedGeometries(filter: Filter, attribute: String, intersect: Boolean): FilterValues[Geometry] = {
    filter match {
      case o: Or  =>
        val all = o.getChildren.map(extractUnclippedGeometries(_, attribute, intersect))
        val join = FilterValues.or[Geometry]((l, r) => l ++ r) _
        all.reduceLeftOption[FilterValues[Geometry]](join).getOrElse(FilterValues.empty)

      case a: And =>
        val all = a.getChildren.map(extractUnclippedGeometries(_, attribute, intersect)).filter(_.nonEmpty)
        if (intersect) {
          val intersect = FilterValues.and[Geometry]((l, r) => Option(l.intersection(r)).filterNot(_.isEmpty)) _
          all.reduceLeftOption[FilterValues[Geometry]](intersect).getOrElse(FilterValues.empty)
        } else {
          FilterValues(all.flatMap(_.values))
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
        FilterValues(geometry.map(flattenGeometry).getOrElse(Seq.empty))

      case _ => FilterValues.empty
    }
  }

  private def flattenGeometry(geometry: Geometry): Seq[Geometry] = geometry match {
    case g: GeometryCollection => (0 until g.getNumGeometries).map(g.getGeometryN).flatMap(flattenGeometry)
    case _ => Seq(geometry)
  }

  /**
    * Extracts intervals from a filter. Intervals will be merged where possible - the resulting sequence
    * is considered to be a union (i.e. OR)
    *
    * @param filter filter to evaluate
    * @param attribute attribute to consider
    * @param intersect intersect extracted values together, or return them all
    *                  note if not intersected, 'and/or' distinction will be lost
    * @return a sequence of intervals, if any. disjoint intervals will result in Seq((null, null))
    */
  def extractIntervals(filter: Filter,
                       attribute: String,
                       intersect: Boolean = true,
                       handleExclusiveBounds: Boolean = false): FilterValues[(DateTime, DateTime)] = {

    def roundSecondsUp(dt: DateTime): DateTime = dt.plusSeconds(1).withMillisOfSecond(0)
    def roundSecondsDown(dt: DateTime): DateTime = {
      val millis = dt.getMillisOfSecond
      if (millis == 0) dt.minusSeconds(1) else dt.withMillisOfSecond(0)
    }

    extractAttributeBounds(filter, attribute, classOf[Date]).map { bounds =>
      def roundLo(dt: DateTime) = if (handleExclusiveBounds && !bounds.inclusive) roundSecondsUp(dt) else dt
      def roundUp(dt: DateTime) = if (handleExclusiveBounds && !bounds.inclusive) roundSecondsDown(dt) else dt

      val lower = bounds.lower.map(new DateTime(_, DateTimeZone.UTC)).map(roundLo).getOrElse(MinDateTime)
      val upper = bounds.upper.map(new DateTime(_, DateTimeZone.UTC)).map(roundUp).getOrElse(MaxDateTime)

      (lower, upper)
    }
  }

  /**
    * Extracts bounds from filters that pertain to a given attribute. Bounds will be merged where
    * possible.
    *
    * @param filter filter to evaluate
    * @param attribute attribute name to consider
    * @param binding attribute type
    * @param intersect intersect resulting values, or return all separately
    *                  note if not intersected, 'and/or' distinction will be lost
    * @return a sequence of bounds, if any
    */
  def extractAttributeBounds[T](filter: Filter,
                                attribute: String,
                                binding: Class[T],
                                intersect: Boolean = true): FilterValues[Bounds[T]] = {
    filter match {
      case o: Or =>
        val all = o.getChildren.map(extractAttributeBounds(_, attribute, binding)).filter(_.nonEmpty)
        val join = FilterValues.or[Bounds[T]](Bounds.union[T]) _
        all.reduceLeftOption[FilterValues[Bounds[T]]](join).getOrElse(FilterValues.empty)

      case a: And =>
        val all = a.getChildren.map(extractAttributeBounds(_, attribute, binding)).filter(_.nonEmpty)
        if (intersect) {
          val intersection = FilterValues.and[Bounds[T]](Bounds.intersection[T]) _
          all.reduceLeftOption[FilterValues[Bounds[T]]](intersection).getOrElse(FilterValues.empty)
        } else {
          FilterValues(all.flatMap(_.values))
        }

      case f: PropertyIsEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            FilterValues(Seq(Bounds(Some(lit), Some(lit), inclusive = true)))
          }
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsBetween =>
        try {
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (prop != attribute) { FilterValues.empty } else {
            val lower = f.getLowerBoundary.evaluate(null, binding)
            val upper = f.getUpperBoundary.evaluate(null, binding)
            // note that between is inclusive
            val bounds = Bounds(Option(lower), Option(upper), inclusive = true)
            FilterValues(Seq(bounds))
          }
        } catch {
          case e: Exception =>
            FilterHelperLogger.log.warn(s"Unable to extract bounds from filter '${filterToString(f)}'", e)
            FilterValues.empty
        }

      case f: During if classOf[Date].isAssignableFrom(binding) =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, classOf[Period])).map { p =>
            val lower = p.getBeginning.getPosition.getDate.asInstanceOf[T]
            val upper = p.getEnding.getPosition.getDate.asInstanceOf[T]
            // note that during is exclusive
            val bounds = Bounds(Option(lower), Option(upper), inclusive = false)
            FilterValues(Seq(bounds))
          }
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsGreaterThan =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (None, Some(lit)) else (Some(lit), None)
            val bounds = Bounds(lower, upper, inclusive = false)
            FilterValues(Seq(bounds))
          }
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsGreaterThanOrEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (None, Some(lit)) else (Some(lit), None)
            val bounds = Bounds(lower, upper, inclusive = true)
            FilterValues(Seq(bounds))
          }
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsLessThan =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (Some(lit), None) else (None, Some(lit))
            val bounds = Bounds(lower, upper, inclusive = false)
            FilterValues(Seq(bounds))
          }
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsLessThanOrEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (Some(lit), None) else (None, Some(lit))
            val bounds = Bounds(lower, upper, inclusive = true)
            FilterValues(Seq(bounds))
          }
        }.getOrElse(FilterValues.empty)

      case f: Before =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (Option(lit), None) else (None, Option(lit))
            // note that before is exclusive
            val bounds = Bounds(lower, upper, inclusive = false)
            FilterValues(Seq(bounds))
          }
        }.getOrElse(FilterValues.empty)

      case f: After =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val (lower, upper) = if (prop.flipped) (None, Option(lit)) else (Option(lit), None)
            // note that after is exclusive
            val bounds = Bounds(lower, upper, inclusive = false)
            FilterValues(Seq(bounds))
          }
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsLike if binding == classOf[String] =>
        try {
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (prop != attribute) { FilterValues.empty } else {
            // Remove the trailing wildcard and create a range prefix
            val literal = f.getLiteral
            val lower = if (literal.endsWith(MULTICHAR_WILDCARD)) {
              literal.substring(0, literal.length - MULTICHAR_WILDCARD.length)
            } else {
              literal
            }
            val upper = Some(lower + WILDCARD_SUFFIX).asInstanceOf[Some[T]]
            val bounds = Bounds(Some(lower.asInstanceOf[T]), upper, inclusive = true)
            FilterValues(Seq(bounds))
          }
        } catch {
          case e: Exception =>
            FilterHelperLogger.log.warn(s"Unable to extract bounds from filter '${filterToString(f)}'", e)
            FilterValues.empty
        }

      case f: Not if f.getFilter.isInstanceOf[PropertyIsNull] =>
        try {
          val isNull = f.getFilter.asInstanceOf[PropertyIsNull]
          val prop = isNull.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (prop != attribute) { FilterValues.empty } else {
            val bounds = Bounds[T](None, None, inclusive = true)
            FilterValues(Seq(bounds))
          }
        } catch {
          case e: Exception =>
            FilterHelperLogger.log.warn(s"Unable to extract bounds from filter '${filterToString(f)}'", e)
            FilterValues.empty
        }

      case f: Not =>
        // we extract the sub-filter bounds, then invert them
        val inverted = extractAttributeBounds(f.getFilter, attribute, binding)
        if (inverted.isEmpty) {
          inverted
        } else if (inverted.disjoint) {
          FilterValues(Seq(Bounds(None, None, inclusive = true))) // equivalent to not null
        } else {
          // NOT(A OR B) turns into NOT(A) AND NOT(B)
          val uninverted = inverted.values.map { bound =>
            // NOT the single bound
            val not = bound.bounds match {
              case (None, None) => Seq.empty
              case (Some(lo), None) => Seq(Bounds(None, Some(lo), !bound.inclusive))
              case (None, Some(hi)) => Seq(Bounds(Some(hi), None, !bound.inclusive))
              case (Some(lo), Some(hi)) =>
                Seq(Bounds(None, Some(lo), !bound.inclusive), Bounds(Some(hi), None, !bound.inclusive))
            }
            FilterValues(not)
          }
          // AND together
          val intersect = FilterValues.and[Bounds[T]](Bounds.intersection[T]) _
          uninverted.reduceLeft[FilterValues[Bounds[T]]](intersect)
        }

      case f: TEquals =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(prop.literal.evaluate(null, binding)).map { lit =>
            val bounds = Bounds(Some(lit), Some(lit), inclusive = true)
            FilterValues(Seq(bounds))
          }
        }.getOrElse(FilterValues.empty)

      case _ => FilterValues.empty
    }
  }

  def propertyNames(filter: Filter, sft: SimpleFeatureType): Seq[String] =
    DataUtilities.propertyNames(filter, sft).map(_.getPropertyName).toSeq.sorted

  def hasIdFilter(filter: Filter): Boolean =
    filter.accept(new IdDetectingFilterVisitor, false).asInstanceOf[Boolean]

  def filterListAsAnd(filters: Seq[Filter]): Option[Filter] = andOption(filters)

  /**
    * Simplifies filters to make them easier to process.
    *
    * Current simplifications:
    *
    *   1) Extracts out common parts in an OR clause to simplify further processing.
    *
    *      Example: OR(AND(1, 2), AND(1, 3), AND(1, 4)) -> AND(1, OR(2, 3, 4))
    *
    *   2) N/A - add more simplifications here as needed
    *
    * @param filter filter
    * @return
    */
  def simplify(filter: Filter): Filter = {
    def deduplicateOrs(f: Filter): Filter = f match {
      case and: And => ff.and(and.getChildren.map(deduplicateOrs))

      case or: Or =>
        // OR(AND(1,2,3), AND(1,2,4)) -> Seq(Seq(1,2,3), Seq(1,2,4))
        val decomposed = or.getChildren.map(decomposeAnd)
        val clauses = decomposed.head // Seq(1,2,3)
        val duplicates = clauses.filter(c => decomposed.tail.forall(_.contains(c))) // Seq(1,2)
        if (duplicates.isEmpty) { or } else {
          val deduplicated = orOption(decomposed.flatMap(d => andOption(d.filterNot(duplicates.contains))))
          andFilters(deduplicated.toSeq ++ duplicates)
        }

      case _ => f
    }
    // TODO GEOMESA-1533 simplify ANDs of ORs for DNF
    flatten(deduplicateOrs(flatten(filter)))
  }

  /**
    * Flattens nested ands and ors.
    *
    * Example: AND(1, AND(2, 3)) -> AND(1, 2, 3)
    *
    * @param filter filter
    * @return
    */
  def flatten(filter: Filter): Filter = {
    filter match {
      case and: And  => ff.and(flattenAnd(and.getChildren))
      case or: Or    => ff.or(flattenOr(or.getChildren))
      case f: Filter => f
    }
  }

  private def flattenAnd(filters: Seq[Filter]): ListBuffer[Filter] = {
    val remaining = ListBuffer.empty[Filter] ++ filters
    val result = ListBuffer.empty[Filter]
    do {
      remaining.remove(0) match {
        case f: And => remaining.appendAll(f.getChildren)
        case f      => result.append(flatten(f))
      }
    } while (remaining.nonEmpty)
    result
  }

  private def flattenOr(filters: Seq[Filter]): ListBuffer[Filter] = {
    val remaining = ListBuffer.empty[Filter] ++ filters
    val result = ListBuffer.empty[Filter]
    do {
      remaining.remove(0) match {
        case f: Or => remaining.appendAll(f.getChildren)
        case f     => result.append(flatten(f))
      }
    } while (remaining.nonEmpty)
    result
  }
}

/**
  * Holds values extracted from a filter. Values may be empty, in which case nothing was extracted from
  * the filter. May be marked as 'disjoint', which means that mutually exclusive values were extracted
  * from the filter. This may be checked to short-circuit queries that will not result in any hits.
  *
  * @param values values extracted from the filter. If nothing was extracted, will be empty
  * @param disjoint mutually exclusive values were extracted, e.g. 'a < 1 && a > 2'
  * @tparam T type parameter
  */
case class FilterValues[T](values: Seq[T], disjoint: Boolean = false) {
  def map[U](f: T => U): FilterValues[U] = FilterValues(values.map(f), disjoint)
  def flatMap[U](f: T => GenTraversableOnce[U]): FilterValues[U] = FilterValues(values.flatMap(f), disjoint)
  def foreach[U](f: T => U): Unit = values.foreach(f)
  def filter(f: T => Boolean): FilterValues[T] = FilterValues(values.filter(f), disjoint)
  def nonEmpty: Boolean = values.nonEmpty || disjoint
  def isEmpty: Boolean = !nonEmpty
}

object FilterValues {

  def empty[T]: FilterValues[T] = FilterValues[T](Seq.empty)

  def disjoint[T]: FilterValues[T] = FilterValues[T](Seq.empty, disjoint = true)

  def or[T](join: (Seq[T], Seq[T]) => Seq[T])(left: FilterValues[T], right: FilterValues[T]): FilterValues[T] = {
    (left.disjoint, right.disjoint) match {
      case (false, false) => FilterValues(join(left.values, right.values))
      case (false, true)  => left
      case (true,  false) => right
      case (true,  true)  => FilterValues.disjoint
    }
  }

  def and[T](intersect: (T, T) => Option[T])(left: FilterValues[T], right: FilterValues[T]): FilterValues[T] = {
    if (left.disjoint || right.disjoint) {
      FilterValues.disjoint
    } else {
      val intersections = left.values.flatMap(v => right.values.flatMap(intersect(_, v)))
      if (intersections.isEmpty) {
        FilterValues.disjoint
      } else {
        FilterValues(intersections)
      }
    }
  }

}