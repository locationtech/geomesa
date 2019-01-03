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
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait SpatioTemporalFilterStrategy[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W] extends
    GeoMesaFeatureIndex[DS, F, W] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
  import SpatioTemporalFilterStrategy.StaticCost
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  override def getFilterStrategy(sft: SimpleFeatureType,
                                 filter: Filter,
                                 transform: Option[SimpleFeatureType]): Seq[FilterStrategy[DS, F, W]] = {
    val dtg = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to plan a z3 query but the schema does not have a date")
    }

    if (filter == Filter.INCLUDE) {
      Seq(FilterStrategy(this, None, None))
    } else if (filter == Filter.EXCLUDE) {
      Seq.empty
    } else {
      val (temporal, nonTemporal) = FilterExtractingVisitor(filter, dtg, sft)
      val intervals = temporal.map(FilterHelper.extractIntervals(_, dtg)).getOrElse(FilterValues.empty)

      if (!intervals.disjoint && !intervals.exists(_.isBounded)) {
        // if there aren't any intervals then we would have to do a full table scan
        Seq(FilterStrategy(this, None, Some(filter)))
      } else {
        val (spatial, others) = nonTemporal match {
          case Some(nt) => FilterExtractingVisitor(nt, sft.getGeomField, sft, SpatialFilterStrategy.spatialCheck)
          case None     => (None, None)
        }
        Seq(FilterStrategy(this, andOption((spatial ++ temporal).toSeq), others))
      }
    }
  }

  override def getCost(sft: SimpleFeatureType,
                       stats: Option[GeoMesaStats],
                       filter: FilterStrategy[DS, F, W],
                       transform: Option[SimpleFeatureType]): Long = {
    // https://geomesa.atlassian.net/browse/GEOMESA-1166
    // TODO check date range and use z2 instead if too big
    // TODO also if very small bbox, z2 has ~10 more bits of lat/lon info
    filter.primary match {
      case None => Long.MaxValue
      case Some(f) =>
        val dtg = sft.getDtgField.getOrElse {
          throw new RuntimeException("Trying to plan a z3 query but the schema does not have a date")
        }
        val base = stats.flatMap(_.getCount(sft, f, exact = false)).getOrElse(StaticCost)
        if (FilterHelper.extractGeometries(f, sft.getGeomField).isEmpty) {
          // we can still use this index without a geom, but if there is a date attribute index that will work better
          if (sft.getDescriptor(dtg).isIndexed) { Long.MaxValue } else { base * 2 + 1 }
        } else if (!FilterHelper.extractIntervals(f, sft.getDtgField.get).forall(_.isBoundedBothSides)) {
          base * 2 + 1
        } else {
          base
        }
    }
  }
}

object SpatioTemporalFilterStrategy {

  val StaticCost = 200L

  /**
    * Returns true if the temporal filters create a range with an upper and lower bound
    */
  @deprecated("deprecated with no replacement")
  def isBounded(temporalFilter: Filter, sft: SimpleFeatureType, dtg: String): Boolean = {
    val intervals = FilterHelper.extractIntervals(temporalFilter, dtg)
    intervals.nonEmpty && intervals.values.forall(_.isBoundedBothSides)
  }
}
