/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.security

import java.nio.charset.StandardCharsets

import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility, VisibilityEvaluator}
import org.geotools.factory.{CommonFactoryFinder, GeoTools}
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security._
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.opengis.filter.capability.FunctionName

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@deprecated
object VisibilityFilterFunction {
  val name: FunctionName = new FunctionNameImpl("visibility", classOf[java.lang.Boolean])
  def filter: Filter = {
    val ff = CommonFactoryFinder.getFilterFactory2( GeoTools.getDefaultHints )
    val visibilityFunction = ff.function(VisibilityFilterFunction.name.getFunctionName)
    ff.equals(visibilityFunction, ff.literal(true))
  }
}

@deprecated
class VisibilityFilterFunction
  extends FunctionExpressionImpl(VisibilityFilterFunction.name) {
  private val provider = security.getAuthorizationsProvider(Map.empty[String, java.io.Serializable].asJava, Seq())
  private val auths = provider.getAuthorizations.map(_.getBytes(StandardCharsets.UTF_8))
  private val vizEvaluator = new VisibilityEvaluator(new Authorizations(auths))
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
