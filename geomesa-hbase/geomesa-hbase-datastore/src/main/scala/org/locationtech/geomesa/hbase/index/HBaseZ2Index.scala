/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.filters.Z2HBaseFilter
import org.locationtech.geomesa.index.filters.Z2Filter
import org.locationtech.geomesa.index.index.{Z2Index, Z2ProcessingValues}
import org.opengis.feature.simple.SimpleFeatureType

case object HBaseZ2Index extends HBaseLikeZ2Index with HBasePlatform {

  override protected def createPushDownFilters(ds: HBaseDataStore,
                                               sft: SimpleFeatureType,
                                               filter: HBaseFilterStrategyType,
                                               transform: Option[(String, SimpleFeatureType)]): Seq[(Int, HFilter)] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val z2Filter = Z2Index.currentProcessingValues.map { case Z2ProcessingValues(_, bounds) =>
      val offset = if (sft.isTableSharing) { 2 } else { 1 } // sharing + shard - note: currently sharing is always false
      configureZ2PushDown(bounds, offset)
    }
    super.createPushDownFilters(ds, sft, filter, transform) ++ z2Filter.toSeq
  }

  private def configureZ2PushDown(xy: Seq[(Double, Double, Double, Double)], offset: Int): (Int, HFilter) = {
    val normalizedXY = xy.map { case (xmin, ymin, xmax, ymax) =>
      Array(Z2SFC.lon.normalize(xmin), Z2SFC.lat.normalize(ymin), Z2SFC.lon.normalize(xmax), Z2SFC.lat.normalize(ymax))
    }.toArray

    (Z2HBaseFilter.Priority, new Z2HBaseFilter(new Z2Filter(normalizedXY, 8), offset))
  }
}

trait HBaseLikeZ2Index extends HBaseFeatureIndex with Z2Index[HBaseDataStore, HBaseFeature, Mutation, Query] {
  override val version: Int = 1
}
