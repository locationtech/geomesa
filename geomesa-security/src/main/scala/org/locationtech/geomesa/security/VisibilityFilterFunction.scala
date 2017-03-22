/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import org.apache.accumulo.core.security.{ColumnVisibility, VisibilityEvaluator}
import org.geotools.factory.{GeoTools, CommonFactoryFinder}
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.locationtech.geomesa.security
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.opengis.filter.capability.FunctionName

import scala.collection.JavaConverters._


object VisibilityFilterFunction {
  val name: FunctionName = new FunctionNameImpl("visibility", classOf[java.lang.Boolean])
  def filter: Filter = {
    val ff = CommonFactoryFinder.getFilterFactory2( GeoTools.getDefaultHints )
    val visibilityFunction = ff.function(VisibilityFilterFunction.name.getFunctionName)
    ff.equals(visibilityFunction, ff.literal(true))
  }
}

class VisibilityFilterFunction
  extends FunctionExpressionImpl(VisibilityFilterFunction.name) {
  private val provider = security.getAuthorizationsProvider(Map.empty[String, java.io.Serializable].asJava, Seq())
  private val auths = provider.getAuthorizations
  private val vizEvaluator = new VisibilityEvaluator(auths)
  private val vizCache = collection.concurrent.TrieMap.empty[String, Boolean]

  def evaluateSF(feature: SimpleFeature): java.lang.Boolean = {
    feature.visibility.exists(v => vizCache.getOrElseUpdate(v, vizEvaluator.evaluate(new ColumnVisibility(v))))
  }

  @Override
  override def evaluate(obj: Object): Object = obj match {
    case sf: SimpleFeature => evaluateSF(sf)
    case _ => java.lang.Boolean.FALSE
  }
}
