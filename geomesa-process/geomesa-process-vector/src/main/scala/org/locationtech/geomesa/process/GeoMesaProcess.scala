/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process

import java.util.Collections

import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.visitor.{AbstractCalcResult, FeatureAttributeVisitor, FeatureCalc}
import org.geotools.process.vector.VectorProcess
import org.opengis.filter.expression.Expression

/**
  * Marker trait for dynamic loading of processes
  */
trait GeoMesaProcess extends VectorProcess

/**
  * Common trait for visitors, allows for feature collections to execute processing in a standardized way
  */
trait GeoMesaProcessVisitor extends FeatureCalc with FeatureAttributeVisitor {

  /**
    * Optimized method to run distributed processing. Should set the result, available from `getResult`
    *
    * @param source simple feature source
    * @param query may contain additional filters to apply
    */
  def execute(source: SimpleFeatureSource, query: Query): Unit

  // hook to allow delegation from retyping feature collections
  override def getExpressions: java.util.List[Expression] = Collections.emptyList()
}

case class FeatureResult(results: SimpleFeatureCollection) extends AbstractCalcResult
