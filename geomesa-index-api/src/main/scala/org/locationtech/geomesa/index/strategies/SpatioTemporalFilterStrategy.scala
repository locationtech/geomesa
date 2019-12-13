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
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait SpatioTemporalFilterStrategy[T, U] extends GeoMesaFeatureIndex[T, U] {

  import SpatioTemporalFilterStrategy.StaticCost

  def geom: String
  def dtg: String

  override def getFilterStrategy(filter: Filter,
                                 transform: Option[SimpleFeatureType],
                                 stats: Option[GeoMesaStats]): Option[FilterStrategy] = {

    if (filter == Filter.INCLUDE) {
      Some(FilterStrategy(this, None, None, Long.MaxValue))
    } else if (filter == Filter.EXCLUDE) {
      None
    } else {
      val (temporal, nonTemporal) = FilterExtractingVisitor(filter, dtg, sft)
      val intervals = temporal.map(FilterHelper.extractIntervals(_, dtg)).getOrElse(FilterValues.empty)

      if (!intervals.disjoint && !intervals.exists(_.isBounded)) {
        // if there aren't any intervals then we would have to do a full table scan
        Some(FilterStrategy(this, None, Some(filter), Long.MaxValue))
      } else {
        val (spatial, others) = nonTemporal match {
          case Some(f) => FilterExtractingVisitor(f, geom, sft, SpatialFilterStrategy.spatialCheck)
          case None    => (None, None)
        }
        val primary = andFilters(spatial.toSeq ++ temporal)

        lazy val cost = {
          // we can still use this index with a geom, but if there is a date attribute index that will work better
          if (spatial.isEmpty && AttributeIndex.indexed(sft, dtg)) {
            Long.MaxValue
          } else {
            // TODO check date range and use z2 instead if too big
            // TODO also if very small bbox, z2 has ~10 more bits of lat/lon info
            // https://geomesa.atlassian.net/browse/GEOMESA-1166

            val base = stats.flatMap(_.getCount(sft, primary, exact = false)).getOrElse(StaticCost)
            // de-prioritize non-spatial and one-sided date filters
            if (spatial.isDefined && intervals.forall(_.isBoundedBothSides)) { base } else { base * 2 + 1 }
          }
        }

        Some(FilterStrategy(this, Some(primary), others, cost))
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
