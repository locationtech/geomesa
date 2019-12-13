/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security.filter

import java.nio.charset.StandardCharsets
import java.util.Collections

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils, VisibilityEvaluator}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.capability.FunctionName
import org.opengis.filter.expression.Expression

import scala.util.Try

class VisibilityFilterFunction extends FunctionExpressionImpl(VisibilityFilterFunction.Name) {

  import scala.collection.JavaConverters._

  private val cache = scala.collection.mutable.Map.empty[String, java.lang.Boolean]

  private val auths =
    VisibilityFilterFunction.provider.getAuthorizations.asScala.map(_.getBytes(StandardCharsets.UTF_8))

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
        cache.getOrElseUpdate(vis,
          Try(Boolean.box(VisibilityEvaluator.parse(vis).evaluate(auths))).getOrElse(java.lang.Boolean.FALSE))
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

  private val provider = AuthorizationsProvider.apply(Collections.emptyMap(), Collections.emptyList())
}
