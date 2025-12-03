/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security.filter

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.accumulo.access.AccessEvaluator
import org.geotools.api.filter.Filter
import org.geotools.api.filter.capability.FunctionName
import org.geotools.api.filter.expression.Expression
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter

import java.time.Duration
import scala.util.Try

/**
 * Function to compare auths with vis. Note that this function "fails open", in that if visibilities are not found
 * it will count as visible
 */
class IsVisibleFilterFunction extends FunctionExpressionImpl(IsVisibleFilterFunction.Name) {

  private var auths: String = _
  private var expression: Expression = _

  override def setParameters(params: java.util.List[Expression]): Unit = {
    super.setParameters(params)
    this.auths = params.get(0).evaluate(null, classOf[String])
    if (auths == null) {
      throw new IllegalArgumentException(s"Function $name first argument must be a literal string, got: ${params.get(0)}")
    }
    expression = params.get(1)
  }

  override def evaluate(obj: Object): Object = {
    val vis = expression.evaluate(obj).asInstanceOf[String]
    if (vis == null || vis.isBlank) {
      java.lang.Boolean.TRUE
    } else if (auths.isBlank) {
      java.lang.Boolean.FALSE
    } else {
      IsVisibleFilterFunction.visibilityCache.get(auths -> vis)
    }
  }
}

object IsVisibleFilterFunction {

  val Name: FunctionName =
    new FunctionNameImpl("isVisible",
      classOf[java.lang.Boolean],
      parameter("auths", classOf[String]),
      parameter("visibilities", classOf[String]))

  private val ff = CommonFactoryFinder.getFilterFactory()

  private val visibilityCache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(5)).build(
    new CacheLoader[(String, String), java.lang.Boolean]() {
      override def load(k: (String, String)): java.lang.Boolean =
        Try(Boolean.box(evaluatorCache.get(k._1).canAccess(k._2))).getOrElse(java.lang.Boolean.FALSE)
    }
  )

  private val evaluatorCache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(5)).build(
    new CacheLoader[String, AccessEvaluator]() {
      override def load(auths: String): AccessEvaluator = AccessEvaluator.of(auths.split(','): _*)
    }
  )

  /**
   * Create a filter function for a visibility check on each feature
   *
   * @param auths auths
   * @return
   */
  def visible(auths: Seq[String]): Filter = {
    val vis = ff.function(GetVisibilitiesFilterFunction.Name.getFunctionName)
    ff.equals(ff.function(Name.getFunctionName, ff.literal(auths.mkString(",")), vis), ff.literal(true))
  }
}
