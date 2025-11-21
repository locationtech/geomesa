/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security.filter

import org.apache.accumulo.access.AccessEvaluator
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.api.filter.capability.FunctionName
import org.geotools.api.filter.expression.Expression
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.locationtech.geomesa.security.SecurityUtils

import scala.util.Try

class VisibleFilterFunction extends FunctionExpressionImpl(VisibleFilterFunction.Name) {

  private val cache = scala.collection.mutable.Map.empty[String, java.lang.Boolean]

  private var access: AccessEvaluator = _
  private var expression: Expression = _

  override def setParameters(params: java.util.List[Expression]): Unit = {
    val size = Option(params).map(_.size()).getOrElse(0)
    if (size < 1 || size > 2) {
      throw new IllegalArgumentException(s"Function $name expected 1 or 2 arguments, got $size")
    }
    val auths = params.get(0).evaluate(null, classOf[String])
    if (auths == null || auths.isEmpty) {
      throw new IllegalArgumentException(s"Function $name first argument must be a literal string, got ${params.get(0)}")
    }
    this.params = new java.util.ArrayList[Expression](params)
    this.access = AccessEvaluator.of(auths.split(','): _*)
    if (size == 2) {
      expression = params.get(1)
    } else {
      expression = null
    }
  }

  override def evaluate(obj: Object): Object = obj match {
    case sf: SimpleFeature =>
      val vis = if (expression == null) { SecurityUtils.getVisibility(sf) } else { expression.evaluate(obj).asInstanceOf[String] }
      if (vis == null || vis.isEmpty) { java.lang.Boolean.FALSE } else {
        cache.getOrElseUpdate(vis, Try(Boolean.box(access.canAccess(vis))).getOrElse(java.lang.Boolean.FALSE))
      }

    case _ => java.lang.Boolean.FALSE
  }
}

object VisibleFilterFunction {

  private val ff = CommonFactoryFinder.getFilterFactory()

  val Name: FunctionName =
    new FunctionNameImpl("visible",
      classOf[java.lang.Boolean],
      parameter("auths", classOf[String]),
      parameter("attribute", classOf[String], 0, 1))

  /**
   * Create a filter function for a visibility check on each feature
   *
   * @param auths auths
   * @return
   */
  def visible(auths: Seq[String]): Filter =
    ff.equals(ff.function(Name.getFunctionName, ff.literal(auths.mkString(","))), ff.literal(true))
}
