/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.visitor

import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.spatial._
import org.opengis.filter.{And, Filter, Or}

import scala.collection.JavaConversions._

/**
  * Updates filters to handle IDL, dwithin units, and to remove filters that aren't meaningful
  */
trait SafeTopologicalFilterVisitor extends DuplicatingFilterVisitorAccessor {

  import FilterHelper.{isFilterWholeWorld, rewriteDwithin, visitBBOX, visitBinarySpatialOp}

  def sft: SimpleFeatureType

  override def visit(f: Or, data: AnyRef): AnyRef = {
    val newChildren = f.getChildren.map(_.accept(this, data).asInstanceOf[Filter])
    // INCLUDE OR foo == INCLUDE
    if (newChildren.exists(includeEquivalent)) {
      Filter.INCLUDE
    } else {
      factory(data).or(newChildren)
    }
  }

  override def visit(f: And, data: AnyRef): AnyRef = {
    val children = f.getChildren
    if (children.exists(includeEquivalent)) {
      // INCLUDE AND foo == foo
      // ignore check after map to allow nested ignores to bubble up
      val newChildren = children.map(_.accept(this, data).asInstanceOf[Filter]).filterNot(includeEquivalent)
      if (newChildren.isEmpty) {
        Filter.INCLUDE
      } else if (newChildren.length == 1) {
        newChildren.head
      } else {
        factory(data).and(newChildren)
      }
    } else {
      super.visit(f, data)
    }
  }

  override def visit(f: DWithin, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) Filter.INCLUDE else rewriteDwithin(f)

  override def visit(f: BBOX, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) Filter.INCLUDE else visitBBOX(f, sft)

  override def visit(f: Within, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) Filter.INCLUDE else visitBinarySpatialOp(f, sft)

  override def visit(f: Intersects, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) Filter.INCLUDE else visitBinarySpatialOp(f, sft)

  override def visit(f: Overlaps, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) Filter.INCLUDE else visitBinarySpatialOp(f, sft)

  private def includeEquivalent(f: Filter): Boolean = f == Filter.INCLUDE || isFilterWholeWorld(f)
}

// necessary to expose factory method to abstract trait
class DuplicatingFilterVisitorAccessor extends DuplicatingFilterVisitor {
  def factory(data: AnyRef) = super.getFactory(data)
}

class SafeTopologicalFilterVisitorImpl(val sft: SimpleFeatureType) extends SafeTopologicalFilterVisitor
