/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security.filter

import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.api.filter.capability.FunctionName
import org.geotools.api.filter.expression.Expression
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.locationtech.geomesa.security.SecurityUtils

class GetVisibilitiesFilterFunction extends FunctionExpressionImpl(GetVisibilitiesFilterFunction.Name) {

  private var expression: Expression = _

  override def setParameters(params: java.util.List[Expression]): Unit = {
    val size = params.size()
    if (size > 1) {
      throw new IllegalArgumentException(s"Function $name expected 0 or 1 arguments, got $size")
    }
    this.params = new java.util.ArrayList[Expression](params)
    if (size == 1) {
      expression = params.get(0)
    } else {
      expression = null
    }
  }

  override def evaluate(obj: Object): Object = obj match {
    case sf: SimpleFeature =>
      val vis = if (expression == null) { SecurityUtils.getVisibility(sf) } else { expression.evaluate(obj).asInstanceOf[String] }
      if (vis == null || vis.isBlank) { null } else { vis }

    case _ => null
  }
}

object GetVisibilitiesFilterFunction {

  val Name: FunctionName =
    new FunctionNameImpl("getVisibilities", classOf[String], parameter("attribute", classOf[String], 0, 1))

  private val ff = CommonFactoryFinder.getFilterFactory()

  /**
   * Create a filter function for a visibility check on each feature
   *
   * @param auths auths
   * @return
   */
  def visible(auths: Seq[String]): Filter =
    ff.equals(ff.function(Name.getFunctionName.getLocalPart, ff.literal(auths.mkString(","))), ff.literal(true))
}
