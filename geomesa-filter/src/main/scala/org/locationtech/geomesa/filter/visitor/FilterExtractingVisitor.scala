/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.visitor

import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.spatial.{DWithin, _}
import org.opengis.filter.temporal.{Before, Ends, Meets, TOverlaps, _}
import org.opengis.filter.{ExcludeFilter, Or, _}

import scala.collection.JavaConversions._

/**
  * Extracts filters for a given attribute
  */
object FilterExtractingVisitor {

  /**
    * Extract filters on a given attribute. If a schema is available,
    * prefer `apply(Filter, String, SimpleFeatureType, Predicate)` as that will handle
    * things like default geometry bboxes
    *
    * @param filter filter to evaluate
    * @param attribute attribute to extract
    * @param predicate additional predicate on the filters matching the attribute
    * @return (filter on the attribute, filter on everything else)
    */
  def apply(filter: Filter,
            attribute: String,
            predicate: Filter => Boolean): (Option[Filter], Option[Filter]) =
    apply(filter, attribute, null, predicate)

  /**
    * Extract filters on a given attribute
    *
    * @param filter filter to evaluate
    * @param attribute attribute to extract
    * @param sft simple feature type
    * @param predicate additional predicate on the filters matching the attribute
    * @return (filter on the attribute, filter on everything else)
    */
  def apply(filter: Filter,
            attribute: String,
            sft: SimpleFeatureType,
            predicate: Filter => Boolean = _ => true): (Option[Filter], Option[Filter]) = {
    val visitor = new FilterExtractingVisitor(attribute, sft, predicate)
    val (yes, no) = filter.accept(visitor, null).asInstanceOf[(Filter, Filter)]
    (Option(yes), Option(no))
  }
}

class FilterExtractingVisitor(attribute: String, sft: SimpleFeatureType, predicate: Filter => Boolean)
    extends FilterVisitor {

  import org.locationtech.geomesa.filter.{andOption, ff, orOption}

  /**
    * Should we keep the filter or discard it
    *
    * @param f filter
    * @return true if it contains the attribute we want
    */
  private def keep(f: Filter): Boolean = FilterHelper.propertyNames(f, sft).contains(attribute) && predicate(f)

  override def visit(f: Or, data: AnyRef): AnyRef = {
    val children = f.getChildren.map(_.accept(this, data).asInstanceOf[(Filter, Filter)])
    val yes = children.map(_._1).filter(_ != null)
    val no  = children.map(_._2).filter(_ != null)
    // only return ORs where both sides satisfy the predicate
    // we handle the returned predicates as implicit ANDs, if we split an OR we break that
    if (no.isEmpty) {
      (orOption(yes).orNull, null)
    } else {
      (null, orOption(yes ++ no).orNull)
    }
  }

  override def visit(f: And, data: AnyRef): AnyRef = {
    val children = f.getChildren.map(_.accept(this, data).asInstanceOf[(Filter, Filter)])
    val yes = children.map(_._1).filter(_ != null)
    val no  = children.map(_._2).filter(_ != null)
    (andOption(yes).orNull, andOption(no).orNull)
  }

  override def visit(f: Not, data: AnyRef): AnyRef = {
    if (predicate(f)) {
      val (yes, no) = f.getFilter.accept(this, data).asInstanceOf[(Filter, Filter)]
      (if (yes == null) null else ff.not(yes), if (no == null) null else ff.not(no))
    } else {
      (null, f)
    }
  }

  override def visit(f: IncludeFilter, data: AnyRef): AnyRef = (null, f)

  override def visit(f: ExcludeFilter, data: AnyRef): AnyRef = (null, f)

  override def visit(f: Id, data: AnyRef): AnyRef = (null, f)

  override def visit(f: Meets, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Ends, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: EndedBy, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: During, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: BegunBy, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Begins, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Before, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: AnyInteracts, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: After, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Within, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Touches, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: MetBy, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: OverlappedBy, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: TContains, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: TEquals, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: TOverlaps, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsLessThanOrEqualTo, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsLessThan, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsGreaterThanOrEqualTo, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsGreaterThan, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsNotEqualTo, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsEqualTo, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsBetween, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsLike, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsNull, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: PropertyIsNil, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: BBOX, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Beyond, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Contains, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Crosses, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Disjoint, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: DWithin, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Equals, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Intersects, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visit(f: Overlaps, data: AnyRef): AnyRef = if (keep(f)) (f, null) else (null, f)

  override def visitNullFilter(data: AnyRef): AnyRef = (null, null)
}
