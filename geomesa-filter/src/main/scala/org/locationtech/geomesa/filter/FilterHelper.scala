/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter._
import org.geotools.api.filter.expression.{Expression, PropertyName}
import org.geotools.api.filter.spatial._
import org.geotools.api.filter.temporal.{After, Before, During, TEquals}
import org.geotools.api.temporal.Period
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter.expression.AttributeExpression.{FunctionLiteral, PropertyLiteral}
import org.locationtech.geomesa.filter.visitor.IdDetectingFilterVisitor
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.jts.geom._

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.{Date, Locale}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object FilterHelper {

  import org.locationtech.geomesa.utils.geotools.WholeWorldPolygon

  // helper shim to let other classes avoid importing FilterHelper.logger
  object FilterHelperLogger extends LazyLogging {
    private [FilterHelper] def log = logger
  }

  val ff: FilterFactory = org.locationtech.geomesa.filter.ff

  def isFilterWholeWorld(f: Filter): Boolean = f match {
      case op: BBOX       => isOperationGeomWholeWorld(op)
      case op: Intersects => isOperationGeomWholeWorld(op)
      case op: Overlaps   => isOperationGeomWholeWorld(op)
      case op: Within     => isOperationGeomWholeWorld(op, SpatialOpOrder.PropertyFirst)
      case op: Contains   => isOperationGeomWholeWorld(op, SpatialOpOrder.LiteralFirst)
      case _ => false
    }

  private def isOperationGeomWholeWorld[Op <: BinarySpatialOperator]
      (op: Op, order: SpatialOpOrder.SpatialOpOrder = SpatialOpOrder.AnyOrder): Boolean = {
    val prop = checkOrder(op.getExpression1, op.getExpression2)
    // validate that property and literal are in the specified order
    prop.exists { p =>
      val ordered = order match {
        case SpatialOpOrder.AnyOrder      => true
        case SpatialOpOrder.PropertyFirst => !p.flipped
        case SpatialOpOrder.LiteralFirst  => p.flipped
      }
      ordered && Option(FastConverter.evaluate(p.literal, classOf[Geometry])).exists(isWholeWorld)
    }
  }

  def isWholeWorld[G <: Geometry](g: G): Boolean = g != null && g.union.covers(WholeWorldPolygon)

  /**
    * Returns the intersection of this geometry with the world polygon
    *
    * Note: may return the geometry itself if it is already covered by the world
    *
    * @param g geometry
    * @return
    */
  def trimToWorld(g: Geometry): Geometry =
    if (WholeWorldPolygon.covers(g)) { g } else { g.intersection(WholeWorldPolygon) }

  /**
    * Add way points to a geometry, preventing it from being split by JTS AM handling
    *
    * @param g geom
    * @return
    */
  def addWayPointsToBBOX(g: Geometry): Geometry = {
    val geomArray = g.getCoordinates
    val correctedGeom = GeometryUtils.addWayPoints(geomArray).toArray
    if (geomArray.length == correctedGeom.length) { g } else { g.getFactory.createPolygon(correctedGeom) }
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
    extractUnclippedGeometries(filter, attribute, intersect).map(trimToWorld)

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
        val all = o.getChildren.asScala.map(extractUnclippedGeometries(_, attribute, intersect))
        val join = FilterValues.or[Geometry]((l, r) => l ++ r) _
        all.reduceLeftOption[FilterValues[Geometry]](join).getOrElse(FilterValues.empty)

      case a: And =>
        val all = a.getChildren.asScala.map(extractUnclippedGeometries(_, attribute, intersect)).filter(_.nonEmpty)
        if (intersect) {
          val intersect = FilterValues.and[Geometry]((l, r) => Option(l.intersection(r)).filterNot(_.isEmpty)) _
          all.reduceLeftOption[FilterValues[Geometry]](intersect).getOrElse(FilterValues.empty)
        } else {
          FilterValues(all.toSeq.flatMap(_.values))
        }

      // Note: although not technically required, all known spatial predicates are also binary spatial operators
      case f: BinarySpatialOperator if isSpatialFilter(f) =>
        FilterValues(GeometryProcessing.extract(f, attribute))

      case _ =>
        FilterValues.empty
    }
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
                       handleExclusiveBounds: Boolean = false): FilterValues[Bounds[ZonedDateTime]] = {
    extractAttributeBounds(filter, attribute, classOf[Date]).map { bounds =>
      var lower, upper: Bound[ZonedDateTime] = null
      // this if check determines if rounding will be used and if we need to account for narrow ranges
      if (!handleExclusiveBounds || bounds.lower.value.isEmpty || bounds.upper.value.isEmpty ||
          (bounds.lower.inclusive && bounds.upper.inclusive)) {
        lower = createDateTime(bounds.lower, roundSecondsUp, handleExclusiveBounds)
        upper = createDateTime(bounds.upper, roundSecondsDown, handleExclusiveBounds)
      } else {
        // check for extremely narrow filters where our rounding makes the result out-of-order
        // note: both upper and lower are known to be defined based on hitting this else branch
        val margin = if (bounds.lower.inclusive || bounds.upper.inclusive) { 1000 } else { 2000 }
        val round = bounds.upper.value.get.getTime - bounds.lower.value.get.getTime > margin
        lower = createDateTime(bounds.lower, roundSecondsUp, round)
        upper = createDateTime(bounds.upper, roundSecondsDown, round)
      }
      Bounds(lower, upper)
    }
  }

  private def createDateTime(bound: Bound[Date],
                             round: ZonedDateTime => ZonedDateTime,
                             roundExclusive: Boolean): Bound[ZonedDateTime] = {
    if (bound.value.isEmpty) { Bound.unbounded } else {
      val dt = bound.value.map(d => ZonedDateTime.ofInstant(toInstant(d), ZoneOffset.UTC))
      if (roundExclusive && !bound.inclusive) {
        Bound(dt.map(round), inclusive = true)
      } else {
        Bound(dt, bound.inclusive)
      }
    }
  }

  private def roundSecondsUp(dt: ZonedDateTime): ZonedDateTime = dt.plusSeconds(1).withNano(0)

  private def roundSecondsDown(dt: ZonedDateTime): ZonedDateTime = {
    val nanos = dt.getNano
    if (nanos == 0) { dt.minusSeconds(1) } else { dt.withNano(0) }
  }

  /**
    * Extracts bounds from filters that pertain to a given attribute. Bounds will be merged where
    * possible.
    *
    * @param filter filter to evaluate
    * @param attribute attribute name to consider
    * @param binding attribute type
    * @return a sequence of bounds, if any
    */
  def extractAttributeBounds[T](filter: Filter, attribute: String, binding: Class[T]): FilterValues[Bounds[T]] = {
    filter match {
      case o: Or =>
        val union = FilterValues.or[Bounds[T]](Bounds.union[T]) _
        o.getChildren.asScala.map(f =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
        o.getChildren.map(f =>
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        o.getChildren.map(f =>
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        o.getChildren.map(f =>
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
        o.getChildren.map(f =>
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
        o.getChildren.map(f =>
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
          extractAttributeBounds(f, attribute, binding)
        ).reduceLeft[FilterValues[Bounds[T]]]((acc, child) => {
          if (acc.isEmpty || child.isEmpty) {
            FilterValues.empty
          } else {
            union(acc, child)
          }
        })

      case a: And =>
        val all = a.getChildren.asScala.flatMap { f =>
          val child = extractAttributeBounds(f, attribute, binding)
          if (child.isEmpty) { Seq.empty } else { Seq(child) }
        }
        val intersection = FilterValues.and[Bounds[T]](Bounds.intersection[T]) _
        all.reduceLeftOption[FilterValues[Bounds[T]]](intersection).getOrElse(FilterValues.empty)

      case f: PropertyIsEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap {
          case e: PropertyLiteral =>
            Option(FastConverter.evaluate(e.literal, binding)).map { lit =>
              val bound = Bound(Some(lit), inclusive = true)
              FilterValues(Seq(Bounds(bound, bound)))
            }

          case e: FunctionLiteral => extractFunctionBounds(e, inclusive = true, binding)
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsBetween =>
        try {
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (prop != attribute) { FilterValues.empty } else {
            // note that between is inclusive
            val lower = Bound(Option(FastConverter.evaluate(f.getLowerBoundary, binding)), inclusive = true)
            val upper = Bound(Option(FastConverter.evaluate(f.getUpperBoundary, binding)), inclusive = true)
            FilterValues(Seq(Bounds(lower, upper)))
          }
        } catch {
          case e: Exception =>
            FilterHelperLogger.log.warn(s"Unable to extract bounds from filter '${filterToString(f)}'", e)
            FilterValues.empty
        }

      case f: During if classOf[Date].isAssignableFrom(binding) =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap {
          case e: PropertyLiteral =>
            Option(FastConverter.evaluate(e.literal, classOf[Period])).map { p =>
              // note that during is exclusive
              val lower = Bound(Option(p.getBeginning.getPosition.getDate.asInstanceOf[T]), inclusive = false)
              val upper = Bound(Option(p.getEnding.getPosition.getDate.asInstanceOf[T]), inclusive = false)
              FilterValues(Seq(Bounds(lower, upper)))
            }

          case e: FunctionLiteral => extractFunctionBounds(e, inclusive = false, binding)
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsGreaterThan =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap {
          case e: PropertyLiteral =>
            Option(FastConverter.evaluate(e.literal, binding)).map { lit =>
              val bound = Bound(Some(lit), inclusive = false)
              val (lower, upper) = if (e.flipped) { (Bound.unbounded[T], bound) } else { (bound, Bound.unbounded[T]) }
              FilterValues(Seq(Bounds(lower, upper)))
            }

          case e: FunctionLiteral => extractFunctionBounds(e, inclusive = false, binding)
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsGreaterThanOrEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap {
          case e: PropertyLiteral =>
            Option(FastConverter.evaluate(e.literal, binding)).map { lit =>
              val bound = Bound(Some(lit), inclusive = true)
              val (lower, upper) = if (e.flipped) { (Bound.unbounded[T], bound) } else { (bound, Bound.unbounded[T]) }
              FilterValues(Seq(Bounds(lower, upper)))
            }

          case e: FunctionLiteral => extractFunctionBounds(e, inclusive = true, binding)
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsLessThan =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap {
          case e: PropertyLiteral =>
            Option(FastConverter.evaluate(e.literal, binding)).map { lit =>
              val bound = Bound(Some(lit), inclusive = false)
              val (lower, upper) = if (e.flipped) { (bound, Bound.unbounded[T]) } else { (Bound.unbounded[T], bound) }
              FilterValues(Seq(Bounds(lower, upper)))
            }

          case e: FunctionLiteral => extractFunctionBounds(e, inclusive = false, binding)
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsLessThanOrEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap {
          case e: PropertyLiteral =>
            Option(FastConverter.evaluate(e.literal, binding)).map { lit =>
              val bound = Bound(Some(lit), inclusive = true)
              val (lower, upper) = if (e.flipped) { (bound, Bound.unbounded[T]) } else { (Bound.unbounded[T], bound) }
              FilterValues(Seq(Bounds(lower, upper)))
            }

          case e: FunctionLiteral => extractFunctionBounds(e, inclusive = true, binding)
        }.getOrElse(FilterValues.empty)

      case f: Before =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap {
          case e: PropertyLiteral =>
            Option(FastConverter.evaluate(e.literal, binding)).map { lit =>
              // note that before is exclusive
              val bound = Bound(Some(lit), inclusive = false)
              val (lower, upper) = if (e.flipped) { (bound, Bound.unbounded[T]) } else { (Bound.unbounded[T], bound) }
              FilterValues(Seq(Bounds(lower, upper)))
            }

          case e: FunctionLiteral => extractFunctionBounds(e, inclusive = false, binding)
        }.getOrElse(FilterValues.empty)

      case f: After =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap {
          case e: PropertyLiteral =>
            Option(FastConverter.evaluate(e.literal, binding)).map { lit =>
              // note that after is exclusive
              val bound = Bound(Some(lit), inclusive = false)
              val (lower, upper) = if (e.flipped) { (Bound.unbounded[T], bound) } else { (bound, Bound.unbounded[T]) }
              FilterValues(Seq(Bounds(lower, upper)))
            }

          case e: FunctionLiteral => extractFunctionBounds(e, inclusive = false, binding)
        }.getOrElse(FilterValues.empty)

      case f: PropertyIsLike if binding == classOf[String] =>
        try {
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (prop != attribute) { FilterValues.empty } else {
            // find the first wildcard and create a range prefix
            val literal = f.getLiteral
            var i = literal.indexWhere(Wildcards.contains)
            // check for escaped wildcards
            while (i > 1 && literal.charAt(i - 1) == '\\' && literal.charAt(i - 2) == '\\') {
              i = literal.indexWhere(Wildcards.contains, i + 1)
            }
            if (i == -1) {
              val literals = if (f.isMatchingCase) { Seq(literal) } else { casePermutations(literal) }
              val bounds = literals.map { lit =>
                val bound = Bound(Some(lit), inclusive = true)
                Bounds(bound, bound)
              }
              FilterValues(bounds.asInstanceOf[Seq[Bounds[T]]], precise = true)
            } else {
              val prefix = literal.substring(0, i)
              val prefixes = if (f.isMatchingCase) { Seq(prefix) } else { casePermutations(prefix) }
              val bounds = prefixes.map { p =>
                Bounds(Bound(Some(p), inclusive = true), Bound(Some(p + WildcardSuffix), inclusive = true))
              }
              // our ranges fully capture the filter if there's a single trailing multi-char wildcard
              val exact = i == literal.length - 1 && literal.charAt(i) == WildcardMultiChar
              FilterValues(bounds.asInstanceOf[Seq[Bounds[T]]], precise = exact)
            }
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
            FilterValues(Seq(Bounds.everything[T]))
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
          FilterValues(Seq(Bounds.everything[T])) // equivalent to not null
        } else if (!inverted.precise) {
          FilterHelperLogger.log.warn(s"Falling back to full table scan for inverted query: '${filterToString(f)}'")
          FilterValues(Seq(Bounds.everything[T]), precise = false)
        } else {
          // NOT(A OR B) turns into NOT(A) AND NOT(B)
          val uninverted = inverted.values.map { bounds =>
            // NOT the single bound
            val not = bounds.bounds match {
              case (None, None) => Seq.empty
              case (Some(lo), None) => Seq(Bounds(Bound.unbounded, Bound(Some(lo), !bounds.lower.inclusive)))
              case (None, Some(hi)) => Seq(Bounds(Bound(Some(hi), !bounds.upper.inclusive), Bound.unbounded))
              case (Some(lo), Some(hi)) => Seq(
                  Bounds(Bound.unbounded, Bound(Some(lo), !bounds.lower.inclusive)),
                  Bounds(Bound(Some(hi), !bounds.upper.inclusive), Bound.unbounded)
                )
            }
            FilterValues(not)
          }
          // AND together
          val intersect = FilterValues.and[Bounds[T]](Bounds.intersection[T]) _
          uninverted.reduceLeft[FilterValues[Bounds[T]]](intersect)
        }

      case f: TEquals =>
        checkOrder(f.getExpression1, f.getExpression2).filter(_.name == attribute).flatMap { prop =>
          Option(FastConverter.evaluate(prop.literal, binding)).map { lit =>
            val bound = Bound(Some(lit), inclusive = true)
            FilterValues(Seq(Bounds(bound, bound)))
          }
        }.getOrElse(FilterValues.empty)

      case _ => FilterValues.empty
    }
  }

  private def extractFunctionBounds[T](function: FunctionLiteral,
                                       inclusive: Boolean,
                                       binding: Class[T]): Option[FilterValues[Bounds[T]]] = {
    // TODO GEOMESA-1990 extract some meaningful bounds from the function
    Some(FilterValues(Seq(Bounds.everything[T]), precise = false))
  }

  /**
   * Calculates all the different case permutations of a string.
   *
   * For example, "foo" -> Seq("foo", "Foo", "fOo", "foO", "fOO", "FoO", "FOo", "FOO")
   *
   * @param string input string
   * @return
   */
  private def casePermutations(string: String): Seq[String] = {
    val max = FilterProperties.CaseInsensitiveLimit.toInt.getOrElse {
      // has a valid default value so should never return a none
      throw new IllegalStateException(
        s"Error getting default value for ${FilterProperties.CaseInsensitiveLimit.property}")
    }

    val lower = string.toLowerCase(Locale.US)
    val upper = string.toUpperCase(Locale.US)
    // account for chars without upper/lower cases, which we don't need to permute
    val count = (0 until lower.length).count(i => lower(i) != upper(i))

    if (count > max) {
      FilterHelperLogger.log.warn(s"Not expanding case-insensitive prefix due to length: $string")
      Seq.empty
    } else {
      // there will be 2^n different permutations, accounting for chars that don't have an upper/lower case
      val permutations = Array.fill(math.pow(2, count).toInt)(Array(lower: _*))
      var i = 0 // track the index of the current char
      var c = 0 // track the index of the bit check, which skips chars that don't have an upper/lower case
      while (i < string.length) {
        val upperChar = upper.charAt(i)
        if (lower.charAt(i) != upperChar) {
          var j = 0
          while (j < permutations.length) {
            // set upper/lower based on the bit
            if (((j >> c) & 1) != 0) {
              permutations(j)(i) = upperChar
            }
            j += 1
          }
          c += 1
        }
        i += 1
      }

      permutations.map(new String(_))
    }
  }

  /**
    * Extract property names from a filter. If a schema is available,
    * prefer `propertyNames(Filter, SimpleFeatureType)` as that will handle
    * things like default geometry bboxes
    *
    * @param filter filter
    * @return unique property names referenced in the filter, in sorted order
    */
  def propertyNames(filter: Filter): Seq[String] = propertyNames(filter, null)

  /**
    * Extract property names from a filter
    *
    * @param filter filter
    * @param sft simple feature type
    * @return unique property names referenced in the filter, in sorted order
    */
  def propertyNames(filter: Filter, sft: SimpleFeatureType): Seq[String] =
    DataUtilities.attributeNames(filter, sft).toSeq.distinct.sorted

  def propertyNames(expression: Expression, sft: SimpleFeatureType): Seq[String] =
    DataUtilities.attributeNames(expression, sft).toSeq.distinct.sorted

  def hasIdFilter(filter: Filter): Boolean =
    filter.accept(new IdDetectingFilterVisitor, false).asInstanceOf[Boolean]

  def filterListAsAnd(filters: Seq[Filter]): Option[Filter] = andOption(filters)

  def filterListAsOr(filters: Seq[Filter]): Option[Filter] = orOption(filters)

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
      case and: And => ff.and(and.getChildren.asScala.map(deduplicateOrs).asJava)

      case or: Or =>
        // OR(AND(1,2,3), AND(1,2,4)) -> Seq(Seq(1,2,3), Seq(1,2,4))
        val decomposed = or.getChildren.asScala.map(decomposeAnd)
        val clauses = decomposed.head // Seq(1,2,3)
        val duplicates = clauses.filter(c => decomposed.tail.forall(_.contains(c))) // Seq(1,2)
        if (duplicates.isEmpty) { or } else {
          val simplified = decomposed.flatMap(d => andOption(d.filterNot(duplicates.contains)))
          if (simplified.length < decomposed.length) {
            // the duplicated filters are an entire clause, so we can ignore the rest of the clauses
            andFilters(duplicates)
          } else {
            andFilters(orOption(simplified.toSeq).toSeq ++ duplicates)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
            andFilters(orOption(simplified).toSeq ++ duplicates)
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
            andFilters(orOption(simplified).toSeq ++ duplicates)
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
            andFilters(orOption(simplified).toSeq ++ duplicates)
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
            andFilters(orOption(simplified).toSeq ++ duplicates)
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
            andFilters(orOption(simplified).toSeq ++ duplicates)
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
          }
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
      case and: And  => ff.and(flattenAnd(and.getChildren.asScala.toSeq).asJava)
      case or: Or    => ff.or(flattenOr(or.getChildren.asScala.toSeq).asJava)
      case f: Filter => f
    }
  }

  private [filter] def flattenAnd(filters: Seq[Filter]): ListBuffer[Filter] = {
    val remaining = ListBuffer.empty[Filter] ++ filters
    val result = ListBuffer.empty[Filter]
    while (remaining.nonEmpty) {
      remaining.remove(0) match {
        case f: And => remaining.appendAll(f.getChildren.asScala)
        case f      => result.append(flatten(f))
      }
    }
    result
  }

  private [filter] def flattenOr(filters: Seq[Filter]): ListBuffer[Filter] = {
    val remaining = ListBuffer.empty[Filter] ++ filters
    val result = ListBuffer.empty[Filter]
    while (remaining.nonEmpty) {
      remaining.remove(0) match {
        case f: Or => remaining.appendAll(f.getChildren.asScala)
        case f     => result.append(flatten(f))
      }
    }
    result
  }

  private object SpatialOpOrder extends Enumeration {
    type SpatialOpOrder = Value
    val PropertyFirst, LiteralFirst, AnyOrder = Value
  }
}
