/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.strategies

import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, Or}

trait SpatialFilterStrategy[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
    extends GeoMesaFeatureIndex[DS, F, W] {

  import SpatialFilterStrategy.{StaticCost, spatialCheck}

  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[FilterStrategy[DS, F, W]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    if (filter == Filter.INCLUDE) {
      Seq(FilterStrategy(this, None, None))
    } else if (filter == Filter.EXCLUDE) {
      Seq.empty
    } else {
      val (spatial, nonSpatial) = FilterExtractingVisitor(filter, sft.getGeomField, sft, spatialCheck)
      if (spatial.nonEmpty) {
        Seq(FilterStrategy(this, spatial, nonSpatial))
      } else {
        Seq(FilterStrategy(this, None, Some(filter)))
      }
    }
  }

  override def getCost(sft: SimpleFeatureType,
                       ds: Option[DS],
                       filter: FilterStrategy[DS, F, W],
                       transform: Option[SimpleFeatureType]): Long = {
    filter.primary match {
      case None    => Long.MaxValue
      case Some(f) =>
        // add one so that we prefer the z3 index even if geometry is the limiting factor, resulting in the same count
        ds.flatMap(_.stats.getCount(sft, f, exact = false).map(c => if (c == 0L) 0L else c + 1L)).getOrElse(StaticCost)
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
      case f: And => true // note: implies further evaluation of children
      case f: Or  => true // note: implies further evaluation of children
      case _ => isSpatialFilter(filter)
    }
  }
}
