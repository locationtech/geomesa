/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.visitor

import org.geotools.filter.LiteralExpressionImpl
import org.geotools.filter.visitor.{DuplicatingFilterVisitor, IsStaticExpressionVisitor}
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.spatial._
import org.opengis.filter.{And, Filter, Or}

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Updates filters to handle namespaces, default property names, IDL, dwithin units,
  * and to remove filters that aren't meaningful
  */
class QueryPlanFilterVisitor(sft: SimpleFeatureType) extends DuplicatingFilterVisitor {

  import FilterHelper.{isFilterWholeWorld, visitBinarySpatialOp}

  override def visit(f: Or, data: AnyRef): AnyRef = {
    val newChildren = f.getChildren.map(_.accept(this, data).asInstanceOf[Filter])
    // INCLUDE OR foo == INCLUDE
    if (newChildren.exists(includeEquivalent)) {
      Filter.INCLUDE
    } else {
      val notExcludes = newChildren.filterNot(_ == Filter.EXCLUDE)
      // EXCLUDE OR foo == foo
      if (notExcludes.isEmpty) {
        Filter.EXCLUDE
      } else {
        getFactory(data).or(notExcludes)
      }
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
      } else if (newChildren.contains(Filter.EXCLUDE)) {
        // EXCLUDE AND foo == EXCLUDE
        Filter.EXCLUDE
      } else {
        getFactory(data).and(newChildren)
      }
    } else {
      super.visit(f, data)
    }
  }

  // note: for the following filters, we call super.visit first to handle any property names

  override def visit(f: DWithin, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      visitBinarySpatialOp(super.visit(f, data).asInstanceOf[DWithin], sft, getFactory(data))
    }

  override def visit(f: BBOX, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      visitBinarySpatialOp(super.visit(f, data).asInstanceOf[BBOX], sft, getFactory(data))
    }

  override def visit(f: Within, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      visitBinarySpatialOp(super.visit(f, data).asInstanceOf[Within], sft, getFactory(data))
    }

  override def visit(f: Intersects, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      visitBinarySpatialOp(super.visit(f, data).asInstanceOf[Intersects], sft, getFactory(data))
    }

  override def visit(f: Overlaps, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      visitBinarySpatialOp(super.visit(f, data).asInstanceOf[Overlaps], sft, getFactory(data))
    }

  override def visit(f: Contains, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      visitBinarySpatialOp(super.visit(f, data).asInstanceOf[Contains], sft, getFactory(data))
    }
  
  override def visit(expression: PropertyName, extraData: AnyRef): AnyRef = {
    val name = expression.getPropertyName
    if (name == null || name.isEmpty) {
      // use the default geometry name
      val geomName = sft.getGeometryDescriptor.getLocalName
      super.getFactory(extraData).property(geomName, expression.getNamespaceContext)
    } else {
      val index = name.indexOf(':')
      if (index == -1) {
        super.getFactory(extraData).property(name)
      } else {
        // strip off the namespace
        super.getFactory(extraData).property(name.substring(index + 1), expression.getNamespaceContext)
      }
    }
  }

  private def includeEquivalent(f: Filter): Boolean = f == Filter.INCLUDE || isFilterWholeWorld(f)

  override def visit(expression: Expression, extraData: scala.Any): Expression = {
    val exp = super.visit(expression, extraData)

    if (exp.accept(IsStaticExpressionVisitor.VISITOR, null).asInstanceOf[Boolean]) {
      Try(exp.evaluate(null)).filter(_ != null).map(new LiteralExpressionImpl(_)).getOrElse(exp)
    } else {
      exp
    }
  }
}

object QueryPlanFilterVisitor {
  def apply(filter: Filter): Filter = filter.accept(new QueryPlanFilterVisitor(null), null).asInstanceOf[Filter]
  def apply(sft: SimpleFeatureType, filter: Filter): Filter =
    filter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter]
}