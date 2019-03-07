/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.visitor

import java.util.{Collections, Date}

import org.geotools.filter.visitor.{DuplicatingFilterVisitor, ExpressionTypeVisitor, IsStaticExpressionVisitor}
import org.locationtech.geomesa.filter.{FilterHelper, GeometryProcessing}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._
import org.opengis.temporal.Period

import scala.util.{Success, Try}

/**
  * Updates filters to handle namespaces, default property names, IDL, dwithin units,
  * type binding, and to remove filters that aren't meaningful
  */
class QueryPlanFilterVisitor(sft: SimpleFeatureType) extends DuplicatingFilterVisitor {

  import FilterHelper.isFilterWholeWorld
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private val typeVisitor = new ExpressionTypeVisitor(sft) {
    override def visit(expression: PropertyName, extraData: AnyRef): AnyRef = {
      val descriptor = expression.evaluate(sft, classOf[AttributeDescriptor])
      // json attributes can return anything due to json path evaluation, so to avoid binding incorrectly
      // we return classOf[Object] here
      // we have to re-fetch the original descriptor as the json property accessor strips out the json flag
      // to prevent transform serialization issues
      if (descriptor == null || sft.getDescriptor(descriptor.getLocalName).isJson) {
        classOf[Object]
      } else {
        descriptor.getType.getBinding
      }
    }
  }

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

  // note: for the following filters, we call super.visit first to handle any property names

  override def visit(f: DWithin, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      GeometryProcessing.process(super.visit(f, data).asInstanceOf[BinarySpatialOperator], sft, getFactory(data))
    }

  override def visit(f: BBOX, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      GeometryProcessing.process(super.visit(f, data).asInstanceOf[BinarySpatialOperator], sft, getFactory(data))
    }

  override def visit(f: Within, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      GeometryProcessing.process(super.visit(f, data).asInstanceOf[BinarySpatialOperator], sft, getFactory(data))
    }

  override def visit(f: Intersects, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      GeometryProcessing.process(super.visit(f, data).asInstanceOf[BinarySpatialOperator], sft, getFactory(data))
    }

  override def visit(f: Overlaps, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      GeometryProcessing.process(super.visit(f, data).asInstanceOf[BinarySpatialOperator], sft, getFactory(data))
    }

  override def visit(f: Contains, data: AnyRef): AnyRef =
    if (isFilterWholeWorld(f)) { Filter.INCLUDE } else {
      GeometryProcessing.process(super.visit(f, data).asInstanceOf[BinarySpatialOperator], sft, getFactory(data))
    }
  
  override def visit(expression: PropertyName, extraData: AnyRef): AnyRef = {
    val name = expression.getPropertyName
    if (name == null || name.isEmpty) {
      // use the default geometry name
      val geomName = sft.getGeometryDescriptor.getLocalName
      getFactory(extraData).property(geomName, expression.getNamespaceContext)
    } else {
      val index = name.indexOf(':')
      if (index == -1) {
        getFactory(extraData).property(name)
      } else {
        // strip off the namespace
        getFactory(extraData).property(name.substring(index + 1), expression.getNamespaceContext)
      }
    }
  }

  override def visit(filter: PropertyIsEqualTo, extraData: AnyRef): AnyRef = {
    val target = binding(Seq(filter.getExpression1, filter.getExpression2))
    if (target == null) { super.visit(filter, extraData) } else {
      val e1 = bind(filter.getExpression1, extraData, target)
      val e2 = bind(filter.getExpression2, extraData, target)
      getFactory(extraData).equal(e1, e2, filter.isMatchingCase, filter.getMatchAction)
    }
  }

  override def visit(filter: PropertyIsNotEqualTo, extraData: AnyRef): AnyRef = {
    val target = binding(Seq(filter.getExpression1, filter.getExpression2))
    if (target == null) { super.visit(filter, extraData) } else {
      val e1 = bind(filter.getExpression1, extraData, target)
      val e2 = bind(filter.getExpression2, extraData, target)
      getFactory(extraData).notEqual(e1, e2, filter.isMatchingCase, filter.getMatchAction)
    }
  }

  override def visit(filter: PropertyIsBetween, extraData: AnyRef): AnyRef = {
    val target = binding(Seq(filter.getExpression, filter.getLowerBoundary, filter.getUpperBoundary))
    if (target == null) { super.visit(filter, extraData) } else {
      val e = bind(filter.getExpression, extraData, target)
      val lb = bind(filter.getLowerBoundary, extraData, target)
      val ub = bind(filter.getUpperBoundary, extraData, target)
      getFactory(extraData).between(e, lb, ub, filter.getMatchAction)
    }
  }

  override def visit(filter: PropertyIsGreaterThan, extraData: AnyRef): AnyRef = {
    val target = binding(Seq(filter.getExpression1, filter.getExpression2))
    if (target == null) { super.visit(filter, extraData) } else {
      val e1 = bind(filter.getExpression1, extraData, target)
      val e2 = bind(filter.getExpression2, extraData, target)
      getFactory(extraData).greater(e1, e2, filter.isMatchingCase, filter.getMatchAction)
    }
  }

  override def visit(filter: PropertyIsGreaterThanOrEqualTo, extraData: AnyRef): AnyRef = {
    val target = binding(Seq(filter.getExpression1, filter.getExpression2))
    if (target == null) { super.visit(filter, extraData) } else {
      val e1 = bind(filter.getExpression1, extraData, target)
      val e2 = bind(filter.getExpression2, extraData, target)
      getFactory(extraData).greaterOrEqual(e1, e2, filter.isMatchingCase, filter.getMatchAction)
    }
  }

  override def visit(filter: PropertyIsLessThan, extraData: AnyRef): AnyRef = {
    val target = binding(Seq(filter.getExpression1, filter.getExpression2))
    if (target == null) { super.visit(filter, extraData) } else {
      val e1 = bind(filter.getExpression1, extraData, target)
      val e2 = bind(filter.getExpression2, extraData, target)
      getFactory(extraData).less(e1, e2, filter.isMatchingCase, filter.getMatchAction)
    }
  }

  override def visit(filter: PropertyIsLessThanOrEqualTo, extraData: AnyRef): AnyRef = {
    val target = binding(Seq(filter.getExpression1, filter.getExpression2))
    if (target == null) { super.visit(filter, extraData) } else {
      val e1 = bind(filter.getExpression1, extraData, target)
      val e2 = bind(filter.getExpression2, extraData, target)
      getFactory(extraData).lessOrEqual(e1, e2, filter.isMatchingCase, filter.getMatchAction)
    }
  }

  override def visit(filter: After, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).after(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: AnyInteracts, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).anyInteracts(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: Before, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).before(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: Begins, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).begins(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: BegunBy, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).begins(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: During, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).during(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: EndedBy, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).endedBy(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: Ends, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).ends(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: Meets, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).meets(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: MetBy, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).metBy(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: OverlappedBy, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).overlappedBy(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: TContains, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).tcontains(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: TEquals, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).tequals(e1, e2, filter.getMatchAction)
  }

  override def visit(filter: TOverlaps, extraData: AnyRef): AnyRef = {
    val e1 = bind(filter.getExpression1, extraData, classOf[Period], classOf[Date])
    val e2 = bind(filter.getExpression2, extraData, classOf[Period], classOf[Date])
    getFactory(extraData).toverlaps(e1, e2, filter.getMatchAction)
  }

  override def visit(function: Function, extraData: AnyRef): AnyRef = {
    val types = Option(function.getFunctionName).map(_.getArguments.iterator).getOrElse(Collections.emptyIterator())
    val params = function.getParameters.asScala.map { parameter =>
      if (types.hasNext) {
        bind(parameter, extraData, types.next.getType)
      } else {
        visit(parameter, extraData)
      }
    }

    function match {
      case f: InternalFunction => f.duplicate(params: _*)
      case f => getFactory(extraData).function(f.getName, params: _*)
    }
  }

  override protected def visit(expression: Expression, extraData: AnyRef): Expression = {
    if (expression.accept(IsStaticExpressionVisitor.VISITOR, null).asInstanceOf[Boolean]) {
      Try(expression.evaluate(null)) match {
        case Success(lit) if lit != null => getFactory(extraData).literal(lit)
        case _ => super.visit(expression, extraData)
      }
    } else {
      super.visit(expression, extraData)
    }
  }

  private def binding(expressions: Seq[Expression]): Class[_] = {
    val bindings = expressions.flatMap {
      case _: Literal => Seq.empty // don't consider literals, as we're trying to bind them to the right type
      case e => Seq(e.accept(typeVisitor, null)).filter(_ != null)
    }
    bindings.distinct match {
      case Seq(b) => b.asInstanceOf[Class[_]]
      case _ => null // if not exactly one type, we can't bind it
    }
  }

  private def bind(e: Expression, extraData: AnyRef, target: Class[_]): Expression = {
    if (e.isInstanceOf[Literal]) {
      val bound = e.evaluate(null, target)
      if (bound != null) {
        return getFactory(extraData).literal(bound)
      }
    }
    visit(e, extraData)
  }

  private def bind(e: Expression, extraData: AnyRef, target: Class[_], fallback: Class[_]): Expression = {
    if (e.isInstanceOf[Literal]) {
      var bound = e.evaluate(null, target)
      if (bound != null) {
        return getFactory(extraData).literal(bound)
      }
      bound = e.evaluate(null, fallback)
      if (bound != null) {
        return getFactory(extraData).literal(bound)
      }
    }
    visit(e, extraData)
  }
}

object QueryPlanFilterVisitor {
  def apply(filter: Filter): Filter = filter.accept(new QueryPlanFilterVisitor(null), null).asInstanceOf[Filter]
  def apply(sft: SimpleFeatureType, filter: Filter): Filter =
    filter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter]
}