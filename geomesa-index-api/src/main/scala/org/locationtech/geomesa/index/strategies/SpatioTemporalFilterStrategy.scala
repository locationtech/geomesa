/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.strategies

import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait SpatioTemporalFilterStrategy[T, U] extends GeoMesaFeatureIndex[T, U] {

  // attributes are assumed to be a geometry field and a date field
  lazy private val Seq(geom, dtg) = attributes

  override def getFilterStrategy(filter: Filter, transform: Option[SimpleFeatureType]): Option[FilterStrategy] = {
    if (filter == Filter.INCLUDE) {
      Some(FilterStrategy(this, None, None, temporal = false, Float.PositiveInfinity))
    } else {
      val (temporal, nonTemporal) = FilterExtractingVisitor(filter, dtg, sft)
      val intervals = temporal.map(FilterHelper.extractIntervals(_, dtg)).getOrElse(FilterValues.empty)

      if (!intervals.disjoint && !intervals.exists(_.isBounded)) {
        // if there aren't any intervals then we would have to do a full table scan
        Some(FilterStrategy(this, None, Some(filter), temporal = false, Float.PositiveInfinity))
      } else {
        val (spatial, others) = nonTemporal match {
          case Some(f) => FilterExtractingVisitor(f, geom, sft, SpatialFilterStrategy.spatialCheck)
          case None    => (None, None)
        }
        val primary = andFilters(spatial.toSeq ++ temporal)
        // TODO check date range and use z2 instead if too big
        // TODO also if very small bbox, z2 has ~10 more bits of lat/lon info
        // https://geomesa.atlassian.net/browse/GEOMESA-1166

        // de-prioritize non-spatial and one-sided date filters
        val priority = if (spatial.isDefined && intervals.forall(_.isBoundedBothSides)) { 1.1f } else { 3f }
        Some(FilterStrategy(this, Some(primary), others, temporal = true, priority))
      }
    }
  }
}
