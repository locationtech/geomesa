/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.strategies

import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, Or}

trait SpatialFilterStrategy[T, U] extends GeoMesaFeatureIndex[T, U] {

  import SpatialFilterStrategy.{StaticCost, spatialCheck}

  def geom: String

  override def getFilterStrategy(filter: Filter,
                                 transform: Option[SimpleFeatureType],
                                 stats: Option[GeoMesaStats]): Option[FilterStrategy] = {
    if (filter == Filter.INCLUDE) {
      Some(FilterStrategy(this, None, None, Long.MaxValue))
    } else if (filter == Filter.EXCLUDE) {
      None
    } else {
      val (spatial, nonSpatial) = FilterExtractingVisitor(filter, geom, sft, spatialCheck)
      if (spatial.nonEmpty) {
        // add one so that we prefer the z3 index even if geometry is the limiting factor, resulting in the same count
        lazy val cost = stats.flatMap(_.getCount(sft, spatial.get, exact = false).map(c => if (c == 0L) 0L else c + 1L))
        Some(FilterStrategy(this, spatial, nonSpatial, cost.getOrElse(StaticCost)))
      } else {
        Some(FilterStrategy(this, None, Some(filter), Long.MaxValue))
      }
    }
  }
}

object SpatialFilterStrategy {

  val StaticCost = 400L

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
