/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter._
import org.geotools.api.filter.expression.{Expression, Function, Literal, PropertyName}
import org.geotools.api.filter.spatial._
import org.geotools.api.filter.temporal._
import org.geotools.data.DataUtilities
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.expression.AttributeExpression
import org.locationtech.geomesa.filter.expression.AttributeExpression.{FunctionLiteral, PropertyLiteral}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.collection.JavaConverters._
import scala.util.Try

package object filter {

  // Claim: FilterFactory implementations seem to be thread-safe away from
  //  'namespace' and 'function' calls.
  // As such, we can get away with using a shared Filter Factory.
  implicit val ff: FilterFactory = CommonFactoryFinder.getFilterFactory

  object FilterProperties {
    val GeometryProcessing: SystemProperty = SystemProperty("geomesa.geometry.processing", "spatial4j")
    // 10 characters results in 1024 ranges
    val CaseInsensitiveLimit: SystemProperty = SystemProperty("geomesa.ilike.max.length", "10")
  }

  def filterToString(filter: Filter): String = Try(ECQL.toCQL(filter)).getOrElse(filter.toString)
  def filterToString(filter: Option[Filter]): String = filter.map(filterToString).getOrElse("None")
  def filtersToString(filters: Seq[Filter]): String = filters.map(filterToString).mkString(", ")

  /**
   * This function rewrites a org.geotools.api.filter.Filter in terms of a top-level OR with children filters which
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
        ff.and(ll.head.asJava)
      }
    } else {
      val children = ll.map { l =>
        l.size match {
          case 1 => l.head
          case _ => ff.and(l.asJava)
        }
      }
      ff.or(children.asJava)
    }
  }

  /**
   *
   * @param x: An arbitrary @org.geotools.api.filter.Filter
   * @return   A List[ List[Filter] ] where the inner List of Filters are to be joined by
   *           Ands and the outer list combined by Ors.
   */
  private[filter] def logicDistributionDNF(x: Filter): List[List[Filter]] = x match {
    case or: Or  => or.getChildren.asScala.toList.flatMap(logicDistributionDNF)

    case and: And => and.getChildren.asScala.foldRight (List(List.empty[Filter])) {
      (f, dnf) => for {
        a <- logicDistributionDNF (f)
        b <- dnf
      } yield a ++ b
    }

    case not: Not =>
      not.getFilter match {
        case and: And => logicDistributionDNF(deMorgan(and))
        case or:  Or => logicDistributionDNF(deMorgan(or))
        case _: Filter => List(List(not))
      }

    case f: Filter => List(List(f))
  }

  /**
   * This function rewrites a org.geotools.api.filter.Filter in terms of a top-level AND with children filters which
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
    val ll = logicDistributionCNF(FilterHelper.simplify(filter))
    if (ll.size == 1) {
      if (ll.head.size == 1) ll.head.head else ff.or(ll.head.asJava)
    } else {
      val children = ll.map { l =>
        l.size match {
          case 1 => l.head
          case _ => ff.or(l.asJava)
        }
      }
      ff.and(children.asJava)
    }
  }

  /**
   *
   * @param x: An arbitrary @org.geotools.api.filter.Filter
   * @return   A List[ List[Filter] ] where the inner List of Filters are to be joined by
   *           Ors and the outer list combined by Ands.
   */
  def logicDistributionCNF(x: Filter): List[List[Filter]] = x match {
    case and: And => and.getChildren.asScala.toList.flatMap(logicDistributionCNF)

    case or: Or => or.getChildren.asScala.foldRight (List(List.empty[Filter])) {
      (f, cnf) => for {
        a <- logicDistributionCNF(f)
        b <- cnf
      } yield a ++ b
    }

    case not: Not =>
      not.getFilter match {
        case and: And => logicDistributionCNF(deMorgan(and))
        case or:  Or => logicDistributionCNF(deMorgan(or))
        case _: Filter => List(List(not))
      }

    case f: Filter => List(List(f))
  }

  /**
   *  The input is a filter which had a Not applied to it.
   *  This function uses deMorgan's law to 'push the Not down'
   *   as well as cancel adjacent Nots.
   */
  private[filter] def deMorgan(f: Filter)(implicit ff: FilterFactory): Filter = f match {
    case and: And => ff.or(and.getChildren.asScala.map(a => ff.not(a).asInstanceOf[Filter]).asJava)
    case or:  Or  => ff.and(or.getChildren.asScala.map(a => ff.not(a).asInstanceOf[Filter]).asJava)
    case not: Not => not.getFilter
  }

  type PartionedFilter = (Seq[Filter], Seq[Filter])

  // Takes a filter and returns a Seq of Geometric/Topological filters under it.
  //  As a note, currently, only 'good' filters are considered.
  //  The list of acceptable filters is defined by 'spatialFilters'
  //  The notion of 'good' here means *good* to handle to the STII.
  //  Of particular note, we should not give negations to the STII.
  def partitionSubFilters(filter: Filter, filterFilter: Filter => Boolean): PartionedFilter = {
    filter match {
      case a: And => decomposeAnd(a).partition(filterFilter)
      case _      => Seq(filter).partition(filterFilter)
    }
  }

  def partitionPrimarySpatials(filter: Filter, sft: SimpleFeatureType): PartionedFilter =
    partitionSubFilters(filter, isPrimarySpatialFilter(_, sft))

  def partitionPrimarySpatials(filters: Seq[Filter], sft: SimpleFeatureType): PartionedFilter =
    filters.partition(isPrimarySpatialFilter(_, sft))

  def partitionPrimaryTemporals(filters: Seq[Filter], sft: SimpleFeatureType): PartionedFilter = {
    val isTemporal = isPrimaryTemporalFilter(_: Filter, sft)
    filters.partition(isTemporal)
  }

  def partitionID(filter: Filter): (Seq[Filter], Seq[Filter]) = partitionSubFilters(filter, isIdFilter)

  def isIdFilter(f: Filter): Boolean = f.isInstanceOf[Id]

  def isPrimarySpatialFilter(filter: Filter, sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val geom = sft.getGeomField
    val primary = filter match {
      case f: BinarySpatialOperator =>
        checkOrder(f.getExpression1, f.getExpression2)
            .exists(p => p.name == null || p.name.isEmpty || p.name == geom)
      case _ => false
    }
    primary && isSpatialFilter(filter)
  }

  // Defines the topological predicates we like for use in the STII.
  def isSpatialFilter(f: Filter): Boolean = {
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

  // noinspection ExistsEquals
  def isTemporalFilter(f: Filter, dtg: String): Boolean = getAttributeProperty(f).exists(_ == dtg)

  def isPrimaryTemporalFilter(f: Filter, sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.exists(isTemporalFilter(f, _))
  }

  def getAttributeProperty(f: Filter): Option[String] = {
    f match {
      // equals checks
      case f: PropertyIsEqualTo => checkOrder(f.getExpression1, f.getExpression2).map(_.name)
      case f: TEquals           => checkOrder(f.getExpression1, f.getExpression2).map(_.name)

      // like checks
      case f: PropertyIsLike =>
        if (likeEligible(f)) {
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          Some(prop)
        } else {
          None
        }

      // range checks
      case f: PropertyIsGreaterThan          => checkOrder(f.getExpression1, f.getExpression2).map(_.name)
      case f: PropertyIsGreaterThanOrEqualTo => checkOrder(f.getExpression1, f.getExpression2).map(_.name)
      case f: PropertyIsLessThan             => checkOrder(f.getExpression1, f.getExpression2).map(_.name)
      case f: PropertyIsLessThanOrEqualTo    => checkOrder(f.getExpression1, f.getExpression2).map(_.name)
      case f: PropertyIsBetween =>
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        val (left, right) = (f.getLowerBoundary, f.getUpperBoundary) match {
          case (l: Literal, r: Literal) => (l, r)
          case _ => (null, null)
        }
        if (left != null && right != null) {
          Some(prop)
        } else {
          None
        }

      // date range checks
      case f: Before => checkOrder(f.getExpression1, f.getExpression2).map(_.name)
      case f: After  => checkOrder(f.getExpression1, f.getExpression2).map(_.name)
      case f: During => checkOrder(f.getExpression1, f.getExpression2).map(_.name)

      // not check - we only support 'not null' for an indexed attribute
      case n: Not =>
        Option(n.getFilter).collect { case f: PropertyIsNull => f }.map { f =>
          f.getExpression.asInstanceOf[PropertyName].getPropertyName
        }

      case _ => None
    }
  }

  val WildcardMultiChar = '%'
  val WildcardSingleChar = '_'
  val Wildcards = Seq(WildcardMultiChar, WildcardSingleChar)

  val WildcardSuffix = "\uffff\uffff\uffff"

  /**
    * Like queries we can handle with our current index
    *
    * @param filter filter
    * @return
    */
  def likeEligible(filter: PropertyIsLike): Boolean = {
    val lit = filter.getLiteral
    // no leading wildcard (otherwise it's a full table scan)
    lit.nonEmpty && !Wildcards.contains(lit.charAt(0))
  }

  def decomposeBinary(f: Filter): Seq[Filter] =
    f match {
      case b: BinaryLogicOperator => b.getChildren.asScala.toSeq.flatMap(decomposeBinary)
      case f: Filter => Seq(f)
    }

  def decomposeAnd(f: Filter): Seq[Filter] =
    f match {
      case b: And => b.getChildren.asScala.toSeq.flatMap(decomposeAnd)
      case f: Filter => Seq(f)
    }

  def decomposeOr(f: Filter): Seq[Filter] =
    f match {
      case b: Or => b.getChildren.asScala.toSeq.flatMap(decomposeOr)
      case f: Filter => Seq(f)
    }

  def orFilters(filters: Seq[Filter])(implicit ff: FilterFactory = ff): Filter =
    if (filters.lengthCompare(1) == 0) { filters.head } else { ff.or(filters.asJava) }

  def andFilters(filters: Seq[Filter])(implicit ff: FilterFactory = ff): Filter =
    if (filters.lengthCompare(1) == 0) { filters.head } else { ff.and(filters.asJava) }

  def orOption(filters: Seq[Filter])(implicit ff: FilterFactory = ff): Option[Filter] =
    if (filters.lengthCompare(2) < 0) { filters.headOption } else { Some(ff.or(filters.asJava)) }

  def andOption(filters: Seq[Filter])(implicit ff: FilterFactory = ff): Option[Filter] =
    if (filters.lengthCompare(2) < 0) { filters.headOption } else { Some(ff.and(filters.asJava)) }

  def mergeFilters(f1: Filter, f2: Filter): Filter = {
    if (f1 == Filter.INCLUDE) {
      f2
    } else if (f2 == Filter.INCLUDE) {
      f1
    } else if (f1 == f2) {
      f1
    } else {
      ff.and(f1, f2)
    }
  }

  /**
   * Checks the order of properties and literals in the expression
   *
   * @param one first expression
   * @param two second expression
   * @return (prop, literal, whether the order was flipped)
   */
  def checkOrder(one: Expression, two: Expression): Option[AttributeExpression] = {
    (one, two) match {
      case (p: PropertyName, l: Literal) => Some(PropertyLiteral(p.getPropertyName, l, flipped = false))
      case (l: Literal, p: PropertyName) => Some(PropertyLiteral(p.getPropertyName, l, flipped = true))
      case (f: Function, l: Literal)     => attribute(f).map(FunctionLiteral(_, f, l, flipped = false))
      case (l: Literal, f: Function)     => attribute(f).map(FunctionLiteral(_, f, l, flipped = true))

      case (p: PropertyName, f: Function) if attribute(f).isEmpty =>
        Some(PropertyLiteral(p.getPropertyName, ff.literal(f.evaluate(null)), flipped = false))

      case (f: Function, p: PropertyName) if attribute(f).isEmpty =>
        Some(PropertyLiteral(p.getPropertyName, ff.literal(f.evaluate(null)), flipped = true))

      case (f1: Function, f2: Function) =>
        (attribute(f1), attribute(f2)) match {
          case (Some(a), None) => Some(FunctionLiteral(a, f1, ff.literal(f2.evaluate(null)), flipped = false))
          case (None, Some(a)) => Some(FunctionLiteral(a, f2, ff.literal(f1.evaluate(null)), flipped = true))
          case _ => None
        }

      case _ => None
    }
  }

  /**
   * Checks the order of properties and literals in the expression - if the expression does not contain
   * a property and a literal, throws an exception.
   *
   * @param one first expression
    * @param two second expression
    * @return (prop, literal, whether the order was flipped)
   */
  def checkOrderUnsafe(one: Expression, two: Expression): AttributeExpression = {
    checkOrder(one, two).getOrElse {
      throw new RuntimeException(s"Expressions did not contain valid property and literal: $one, $two")
    }
  }

  private def attribute(f: Function): Option[String] =
    f.getParameters.asScala.map(DataUtilities.attributeNames(_).headOption).collectFirst { case Some(p) => p }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    f.getParameters.map(DataUtilities.attributeNames(_).headOption).collectFirst { case Some(p) => p }
<<<<<<< HEAD
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
    f.getParameters.map(DataUtilities.attributeNames(_).headOption).collectFirst { case Some(p) => p }
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7bb5f26c3e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
}
