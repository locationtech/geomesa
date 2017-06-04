/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait SpatioTemporalFilterStrategy[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W] extends
    GeoMesaFeatureIndex[DS, F, W] {

  import SpatioTemporalFilterStrategy.{StaticCost, isBounded}

  override def getFilterStrategy(sft: SimpleFeatureType,
                                 filter: Filter,
                                 transform: Option[SimpleFeatureType]): Seq[FilterStrategy[DS, F, W]] = {

    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val dtg = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to plan a z3 query but the schema does not have a date")
    }

    if (filter == Filter.INCLUDE) {
      Seq(FilterStrategy(this, None, None))
    } else if (filter == Filter.EXCLUDE) {
      Seq.empty
    } else {
      val (temporal, nonTemporal) = FilterExtractingVisitor(filter, dtg, sft)
      val (spatial, others) = nonTemporal match {
        case None     => (None, None)
        case Some(nt) => FilterExtractingVisitor(nt, sft.getGeomField, sft, SpatialFilterStrategy.spatialCheck)
      }

      if (temporal.exists(isBounded(_, dtg)) && (spatial.isDefined || !sft.getDescriptor(dtg).isIndexed)) {
        Seq(FilterStrategy(this, andOption((spatial ++ temporal).toSeq), others))
      } else {
        Seq(FilterStrategy(this, None, Some(filter)))
      }
    }
  }

  override def getCost(sft: SimpleFeatureType,
                       ds: Option[DS],
                       filter: FilterStrategy[DS, F, W],
                       transform: Option[SimpleFeatureType]): Long = {
    // https://geomesa.atlassian.net/browse/GEOMESA-1166
    // TODO check date range and use z2 instead if too big
    // TODO also if very small bbox, z2 has ~10 more bits of lat/lon info
    filter.primary match {
      case None    => Long.MaxValue
      case Some(f) => ds.flatMap(_.stats.getCount(sft, f, exact = false)).getOrElse {
        val names = filter.primary.map(FilterHelper.propertyNames(_, sft)).getOrElse(Seq.empty)
        if (names.contains(sft.getGeomField)) StaticCost else SpatialFilterStrategy.StaticCost + 1
      }
    }
  }
}

object SpatioTemporalFilterStrategy {

  val StaticCost = 200L

  /**
    * Returns true if the temporal filters create a range with an upper and lower bound
    */
  def isBounded(temporalFilter: Filter, dtg: String): Boolean = {
    import FilterHelper.{MaxDateTime, MinDateTime}
    val intervals = FilterHelper.extractIntervals(temporalFilter, dtg)
    intervals.nonEmpty && intervals.values.forall { case (start, end) => start != MinDateTime && end != MaxDateTime }
  }
}
