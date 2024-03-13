/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security.filter

import org.apache.accumulo.access.{AccessEvaluator, Authorizations}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.capability.FunctionName
import org.geotools.api.filter.expression.Expression
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.locationtech.geomesa.security.{AuthUtils, SecurityUtils}

import java.util.Collections
import scala.util.Try

class VisibilityFilterFunction extends FunctionExpressionImpl(VisibilityFilterFunction.Name) {

  private val cache = scala.collection.mutable.Map.empty[String, java.lang.Boolean]

  private val auths = Authorizations.of(VisibilityFilterFunction.provider.getAuthorizations)
  private val access = AccessEvaluator.of(auths)

  private var expression: Expression = _

  override def setParameters(params: java.util.List[Expression]): Unit = {
    super.setParameters(params)
    if (!params.isEmpty) {
      expression = getExpression(0)
    }
  }

  override def evaluate(obj: Object): Object = obj match {
    case sf: SimpleFeature =>
      val vis = if (expression == null) {
        SecurityUtils.getVisibility(sf)
      } else {
        expression.evaluate(obj).asInstanceOf[String]
      }
      if (vis == null || vis.isEmpty) { java.lang.Boolean.FALSE } else {
        cache.getOrElseUpdate(vis, Try(Boolean.box(access.canAccess(vis))).getOrElse(java.lang.Boolean.FALSE))
      }

    case _ => java.lang.Boolean.FALSE
  }
}

object VisibilityFilterFunction {

  val Name: FunctionName =
    new FunctionNameImpl("visibility",
      classOf[java.lang.Boolean],
      parameter("auths", classOf[String]),
      parameter("attribute", classOf[String], 0, 1))

  private val provider = AuthUtils.getProvider(Collections.emptyMap[String, AnyRef](), Seq.empty)
}
