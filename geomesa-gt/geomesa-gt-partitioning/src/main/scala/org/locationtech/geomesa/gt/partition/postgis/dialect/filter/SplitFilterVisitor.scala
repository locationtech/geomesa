/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect.filter

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.FilterAttributeExtractor
import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.filter.{And, Filter, Or}
import org.opengis.filter.expression.Function
import org.opengis.filter.spatial._

/**
 * Filter visitor we use for processing filters in the dialect's `splitFilter`method.
 *
 * Currently the filter does the following:
 * <ul>
 *   <li>replaces constant functions with their literal values</li>
 *   <li>removes "whole-world" geometry filters</li>
 * </ul>
 */
class SplitFilterVisitor(removeWholeWorldFilters: Boolean = true) extends DuplicatingFilterVisitor {

  // replaces 'constant' functions with their literal values
  override def visit(expression: Function, extraData: Any): AnyRef = {
    val extractor = new FilterAttributeExtractor()
    expression.accept(extractor, null)
    if (extractor.isConstantExpression) {
      val literal = expression.evaluate(null)
      SplitFilterVisitor.ff.literal(literal)
    } else {
      super.visit(expression, extraData)
    }
  }

  override def visit(f: DWithin, data: AnyRef): AnyRef =
    if (removeWholeWorldFilters && FilterHelper.isFilterWholeWorld(f)) { Filter.INCLUDE } else { super.visit(f, data) }

  override def visit(f: BBOX, data: AnyRef): AnyRef =
    if (removeWholeWorldFilters && FilterHelper.isFilterWholeWorld(f)) { Filter.INCLUDE } else { super.visit(f, data) }

  override def visit(f: Within, data: AnyRef): AnyRef =
    if (removeWholeWorldFilters && FilterHelper.isFilterWholeWorld(f)) { Filter.INCLUDE } else { super.visit(f, data) }

  override def visit(f: Intersects, data: AnyRef): AnyRef =
    if (removeWholeWorldFilters && FilterHelper.isFilterWholeWorld(f)) { Filter.INCLUDE } else { super.visit(f, data) }

  override def visit(f: Overlaps, data: AnyRef): AnyRef =
    if (removeWholeWorldFilters && FilterHelper.isFilterWholeWorld(f)) { Filter.INCLUDE } else { super.visit(f, data) }

  override def visit(f: Contains, data: AnyRef): AnyRef =
    if (removeWholeWorldFilters && FilterHelper.isFilterWholeWorld(f)) { Filter.INCLUDE } else { super.visit(f, data) }

  override def visit(f: Or, data: AnyRef): AnyRef = {
    val children = new java.util.ArrayList[Filter](f.getChildren.size)
    var i = 0
    while (i < f.getChildren.size) {
      val child = f.getChildren.get(i).accept(this, data).asInstanceOf[Filter]
      if (child == Filter.INCLUDE) {
        // INCLUDE OR foo == INCLUDE
        return Filter.INCLUDE
      } else if (child != Filter.EXCLUDE) {
        // EXCLUDE OR foo == foo
        children.add(child)
      }
      i += 1
    }

    children.size() match {
      case 0 => Filter.EXCLUDE
      case 1 => children.get(0)
      case _ => getFactory(data).or(children)
    }
  }

  override def visit(f: And, data: AnyRef): AnyRef = {
    val children = new java.util.ArrayList[Filter](f.getChildren.size)
    var i = 0
    while (i < f.getChildren.size) {
      val child = f.getChildren.get(i).accept(this, data).asInstanceOf[Filter]
      if (child == Filter.EXCLUDE) {
        // EXCLUDE AND foo == EXCLUDE
        return Filter.EXCLUDE
      } else if (child != Filter.INCLUDE) {
        // INCLUDE AND foo == foo
        children.add(child)
      }
      i += 1
    }
    children.size() match {
      case 0 => Filter.INCLUDE
      case 1 => children.get(0)
      case _ => getFactory(data).and(children)
    }
  }
}

object SplitFilterVisitor {

  private val ff = CommonFactoryFinder.getFilterFactory2()

  def apply(filter: Filter, removeWholeWorldFilters: Boolean): Filter =
    filter.accept(new SplitFilterVisitor(removeWholeWorldFilters), null).asInstanceOf[Filter]
}
