/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo

import org.geotools.factory.CommonFactoryFinder
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Expression, Literal, PropertyName}
import org.opengis.filter.spatial._
import org.opengis.filter.temporal.{BinaryTemporalOperator, TEquals}

import scala.collection.JavaConversions._

package object filter {
  // Claim: FilterFactory implementations seem to be thread-safe away from
  //  'namespace' and 'function' calls.
  // As such, we can get away with using a shared Filter Factory.
  implicit val ff = CommonFactoryFinder.getFilterFactory2

  /**
   * This function rewrites a org.opengis.filter.Filter in terms of a top-level OR with children filters which
   * 1) do not contain further ORs, (i.e., ORs bubble up)
   * 2) only contain at most one AND which is at the top of their 'tree'
   *
   * Note that this further implies that NOTs have been 'pushed down' and do have not have ANDs nor ORs as children.
   *
   * In boolean logic, this form is called disjunctive normal form (DNF).
   *
   * @param filter An arbitrary filter.
   * @return       A filter in DNF (described above).
   */
  def rewriteFilterInDNF(filter: Filter)(implicit ff: FilterFactory): Filter = {
    val ll = logicDistributionDNF(filter)
    if (ll.size == 1) {
      if (ll.head.size == 1) {
        ll.head.head
      } else {
        ff.and(ll.head)
      }
    } else {
      val children = ll.map { l =>
        l.size match {
          case 1 => l.head
          case _ => ff.and(l)
        }
      }
      ff.or(children)
    }
  }

  /**
   *
   * @param x: An arbitrary @org.opengis.filter.Filter
   * @return   A List[ List[Filter] ] where the inner List of Filters are to be joined by
   *           Ands and the outer list combined by Ors.
   */
  private[accumulo] def logicDistributionDNF(x: Filter): List[List[Filter]] = x match {
    case or: Or  => or.getChildren.toList.flatMap(logicDistributionDNF)

    case and: And => and.getChildren.foldRight (List(List.empty[Filter])) {
      (f, dnf) => for {
        a <- logicDistributionDNF (f)
        b <- dnf
      } yield a ++ b
    }

    case not: Not =>
      not.getFilter match {
        case and: And => logicDistributionDNF(deMorgan(and))
        case or:  Or => logicDistributionDNF(deMorgan(or))
        case f: Filter => List(List(not))
      }

    case f: Filter => List(List(f))
  }

  /**
   * This function rewrites a org.opengis.filter.Filter in terms of a top-level AND with children filters which
   * 1) do not contain further ANDs, (i.e., ANDs bubble up)
   * 2) only contain at most one OR which is at the top of their 'tree'
   *
   * Note that this further implies that NOTs have been 'pushed down' and do have not have ANDs nor ORs as children.
   *
   * In boolean logic, this form is called conjunctive normal form (CNF).
   *
   * The main use case for this function is to aid in splitting filters between a combination of a
   * GeoMesa data store and some other data store. This is done with the AndSplittingFilter class.
   * In the examples below, anything with "XAttr" is assumed to be a filter that CANNOT be answered
   * through GeoMesa. In having a filter split on the AND, the portion of the filter that GeoMesa
   * CAN answer will be applied in GeoMesa, returning a result set, and then the portion that GeoMesa CANNOT
   * answer will be applied on that result set.
   *
   * Examples:
   *  1. (
   *       (GmAttr ILIKE 'test')
   *       OR
   *       (date BETWEEN '2014-01-01T10:30:00.000Z' AND '2014-01-02T10:30:00.000Z')
   *     )
   *      AND
   *     (XAttr ILIKE = 'example')
   *
   *     Converting to CNF will allow easily splitting the filter on the AND into two children
   *      - one child is the "GmAttr" and "date" filters that can be answered with GeoMesa
   *      - one child is the "XAttr" filter that cannot be answered by GeoMesa
   *
   *      In this case, the GeoMesa child filter will be processed first, and then the "XAttr" filter will
   *    be processed on the GeoMesa result set to return a subset of the GeoMesa results.
   *
   *  2. (GmAttr ILIKE 'test')
   *      AND
   *          (
   *            (date BETWEEN '2014-01-01T10:30:00.000Z' AND '2014-01-02T10:30:00.000Z')
   *             OR
   *            (XAttr1 ILIKE = 'example1')
   *          )
   *      AND
   *     (XAttr2 ILIKE = 'example2')
   *
   *     Converting to CNF still allows easily splitting the filter on the AND into three children
   *      - one child is the "GmAttr" filter
   *      - one child is the "date" OR "XAttr1" filter
   *      - one child is the "XAttr2" filter
   *
   *      In this case, the "GmAttr" child will be processed first, returning a result set from GeoMesa
   *    called RS1. Then, RS1 will be further filtered with the "date" predicate that can be handled
   *    by GeoMesa, returning a subset of RS1 called SS1. The additional filter which cannot be answered
   *    by GeoMesa, "XAttr1," will be applied to RS1 and return subset SS2. Finally, the final child,
   *    the "XAttr2" filter, which cannot be answered by GeoMesa, will be applied to both SS1 and SS2 to
   *    return SS3, a JOIN of SS1+SS2 filtered with "XAttr2."
   *
   *  3. (GmAttr ILIKE 'test')
   *      OR
   *     (XAttr ILIKE = 'example')
   *
   *     This is the worst-case-scenario for a query that is answered through two data stores, both
   *     GeoMesa and some other store.
   *
   *     CNF converts this to:
   *      - one child of "GmAttr" OR "XAttr"
   *
   *      In this case, the "GmAttr" will return a result set, RS1. The reason this is the
   *    worst-case-scenario is because, to answer the "XAttr" portion of the query (which cannot be
   *    answered by GeoMesa), a "Filter.INCLUDE" A.K.A a full table scan (on Accumulo) A.K.A. every
   *    record in GeoMesa is necessary to find the results that satisfy the "XAttr" portion of the
   *    query. This will product result set RS2. The returned results will be a JOIN of RS1+RS2.
   *
   *
   * @param filter An arbitrary filter.
   * @return       A filter in CNF (described above).
   */
  def rewriteFilterInCNF(filter: Filter)(implicit ff: FilterFactory): Filter = {
    val ll =  logicDistributionCNF(filter)
    if(ll.size == 1) {
      if(ll(0).size == 1) ll(0)(0)
      else ff.or(ll(0))
    }
    else  {
      val children = ll.map { l =>
        l.size match {
          case 1 => l(0)
          case _ => ff.or(l)
        }
      }
      ff.and(children)
    }
  }

  /**
   *
   * @param x: An arbitrary @org.opengis.filter.Filter
   * @return   A List[ List[Filter] ] where the inner List of Filters are to be joined by
   *           Ors and the outer list combined by Ands.
   */
  def logicDistributionCNF(x: Filter): List[List[Filter]] = x match {
    case and: And => and.getChildren.toList.flatMap(logicDistributionCNF)

    case or: Or => or.getChildren.foldRight (List(List.empty[Filter])) {
      (f, cnf) => for {
        a <- logicDistributionCNF(f)
        b <- cnf
      } yield a ++ b
    }

    case not: Not =>
      not.getFilter match {
        case and: And => logicDistributionCNF(deMorgan(and))
        case or:  Or => logicDistributionCNF(deMorgan(or))
        case f: Filter => List(List(not))
      }

    case f: Filter => List(List(f))
  }

  /**
   *  The input is a filter which had a Not applied to it.
   *  This function uses deMorgan's law to 'push the Not down'
   *   as well as cancel adjacent Nots.
   */
  private[accumulo] def deMorgan(f: Filter)(implicit ff: FilterFactory): Filter = f match {
    case and: And => ff.or(and.getChildren.map(a => ff.not(a)))
    case or:  Or  => ff.and(or.getChildren.map(a => ff.not(a)))
    case not: Not => not.getFilter
  }

  // Takes a filter and returns a Seq of Geometric/Topological filters under it.
  //  As a note, currently, only 'good' filters are considered.
  //  The list of acceptable filters is defined by 'spatialFilters'
  //  The notion of 'good' here means *good* to handle to the STII.
  //  Of particular note, we should not give negations to the STII.
  def partitionSubFilters(filter: Filter, filterFilter: Filter => Boolean): (Seq[Filter], Seq[Filter]) = {
    filter match {
      case a: And => a.getChildren.partition(filterFilter)
      case _ => Seq(filter).partition(filterFilter)
    }
  }

  def partitionGeom(filter: Filter, sft: SimpleFeatureType) =
    partitionSubFilters(filter, primarySpatialFilters(_, sft))

  def partitionTemporal(filters: Seq[Filter], dtgAttr: Option[String]): (Seq[Filter], Seq[Filter]) =
    dtgAttr.map { dtga => filters.partition(temporalFilters(dtga)) }.getOrElse((Seq(), filters))

  def partitionID(filter: Filter) = partitionSubFilters(filter, filterIsId)

  def primarySpatialFilters(filter: Filter, sft: SimpleFeatureType): Boolean = {
    val geom = sft.getGeometryDescriptor.getLocalName
    val primary = filter match {
      case f: BinarySpatialOperator =>
        checkOrder(f.getExpression1, f.getExpression2)
            .exists(p => p.name == null || p.name.isEmpty || p.name == geom)
      case _ => false
    }
    primary && spatialFilters(filter)
  }

  // Defines the topological predicates we like for use in the STII.
  def spatialFilters(f: Filter): Boolean = {
    f match {
      case _: BBOX => true
      case _: DWithin => true
      case _: Contains => true
      case _: Crosses => true
      case _: Intersects => true
      case _: Overlaps => true
      case _: Within => true
      case _ => false        // Beyond, Disjoint, DWithin, Equals, Touches
    }
  }

  // This function identifies filters which are either BinaryTemporal or between filters.
  // Either way, we only want to use filters which use the indexed date attribute.
  def temporalFilters(dtgAttr: String): Filter => Boolean = {
    import org.locationtech.geomesa.utils.filters.Typeclasses.BinaryFilter
    import org.locationtech.geomesa.utils.filters.Typeclasses.BinaryFilter.ops

    def eitherSideIsAttribute[B](b: B)(implicit bf: BinaryFilter[B]) =
      dtgAttr == b.left.toString || dtgAttr == b.right.toString

    def filterIsApplicableTemporal: Filter => Boolean = {
      // TEQUALS can't convert to ECQL, so don't consider it here
      case _: TEquals => false
      case bto: BinaryTemporalOperator => eitherSideIsAttribute(bto)
      case _ => false
    }

    def filterIsBetween: Filter => Boolean = {
      case between: PropertyIsBetween => dtgAttr == between.getExpression.toString
      case _ => false
    }

    def filterIsComparisonTemporal: Filter => Boolean = {
      case lt: PropertyIsLessThan             => eitherSideIsAttribute(lt)
      case le: PropertyIsLessThanOrEqualTo    => eitherSideIsAttribute(le)
      case gt: PropertyIsGreaterThan          => eitherSideIsAttribute(gt)
      case ge: PropertyIsGreaterThanOrEqualTo => eitherSideIsAttribute(ge)
      case _ => false
    }

    f => filterIsApplicableTemporal(f) ||
         filterIsBetween(f) ||
         filterIsComparisonTemporal(f)
  }


  def filterIsId(f: Filter): Boolean =
    f match {
      case _: Id => true
      case _     => false
    }


  def decomposeBinary(f: Filter): Seq[Filter] =
    f match {
      case b: BinaryLogicOperator => b.getChildren.toSeq.flatMap(decomposeBinary)
      case f: Filter => Seq(f)
    }

  def decomposeOr(f: Filter): Seq[Filter] =
    f match {
      case b: Or => b.getChildren.toSeq.flatMap(decomposeOr)
      case f: Filter => Seq(f)
    }

  /**
   * Checks the order of properties and literals in the expression
   *
   * @param one
   * @param two
   * @return (prop, literal, whether the order was flipped)
   */
  def checkOrder(one: Expression, two: Expression): Option[PropertyLiteral] =
    (one, two) match {
      case (p: PropertyName, l: Literal) => Some(PropertyLiteral(p.getPropertyName, l, None, flipped = false))
      case (l: Literal, p: PropertyName) => Some(PropertyLiteral(p.getPropertyName, l, None, flipped = true))
      case (_: PropertyName, _: PropertyName) | (_: Literal, _: Literal) => None
      case _ =>
        val msg = s"Unhandled expressions in strategy: ${one.getClass.getName}, ${two.getClass.getName}"
        throw new RuntimeException(msg)

    }

  /**
   * Checks the order of properties and literals in the expression - if the expression does not contain
   * a property and a literal, throws an exception.
   *
   * @param one
   * @param two
   * @return
   */
  def checkOrderUnsafe(one: Expression, two: Expression): PropertyLiteral =
    checkOrder(one, two)
        .getOrElse(throw new RuntimeException("Expressions did not contain valid property and literal"))
}
