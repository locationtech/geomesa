/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.strategies

import org.geotools.api.filter.{And, Filter, Or}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex}

trait SpatialFilterStrategy[T, U] extends GeoMesaFeatureIndex[T, U] {

  import SpatialFilterStrategy.spatialCheck

  // attributes are assumed to be a single geometry field
  lazy private val Seq(geom) = attributes

  override def getFilterStrategy(filter: Filter, hints: Hints): Option[FilterStrategy] = {
    if (filter == Filter.INCLUDE) {
      Some(FilterStrategy(this, None, None, temporal = false, Float.PositiveInfinity, hints))
    } else {
      val (spatial, nonSpatial) = FilterExtractingVisitor(filter, geom, sft, spatialCheck)
      if (spatial.nonEmpty) {
        Some(FilterStrategy(this, spatial, nonSpatial, temporal = false, 1.2f, hints))
      } else {
        Some(FilterStrategy(this, None, Some(filter), temporal = false, Float.PositiveInfinity, hints))
      }
    }
  }
}

object SpatialFilterStrategy {

  /**
    * Evaluates filters that we can handle with the z-index strategies
    *
    * @param filter filter to check
    * @return
    */
  def spatialCheck(filter: Filter): Boolean = {
    filter match {
      case _: And => true // note: implies further evaluation of children
      case _: Or  => true // note: implies further evaluation of children
      case _ => isSpatialFilter(filter)
    }
  }
}
