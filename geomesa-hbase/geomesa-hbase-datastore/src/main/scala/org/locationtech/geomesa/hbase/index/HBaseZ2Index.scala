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
import org.locationtech.geomesa.index.index.BaseFeatureIndex
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2ProcessingValues}
import org.opengis.feature.simple.SimpleFeatureType

case object HBaseZ2Index extends HBaseLikeZ2Index with HBasePlatform

trait HBaseLikeZ2Index extends HBaseFeatureIndex with HBaseZ2PushDown
    with Z2Index[HBaseDataStore, HBaseFeature, Mutation, Query] {
  override val version: Int = 2
}

trait HBaseZ2PushDown extends HBasePlatform {

  this: BaseFeatureIndex[HBaseDataStore, HBaseFeature, Mutation, Query, Z2ProcessingValues] =>

  override protected def createPushDownFilters(ds: HBaseDataStore,
                                               sft: SimpleFeatureType,
                                               filter: HBaseFilterStrategyType,
                                               transform: Option[(String, SimpleFeatureType)]): Seq[(Int, HFilter)] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val z2Filter = keySpace.currentProcessingValues.map { case Z2ProcessingValues(sfc, _, bounds) =>
      val offset = if (sft.isTableSharing) { 2 } else { 1 } // sharing + shard - note: currently sharing is always false
      configureZ2PushDown(sfc, bounds, offset)
    }
    super.createPushDownFilters(ds, sft, filter, transform) ++ z2Filter.toSeq
  }

  private def configureZ2PushDown(sfc: Z2SFC, xy: Seq[(Double, Double, Double, Double)], offset: Int): (Int, HFilter) = {
    val normalizedXY = xy.map { case (xmin, ymin, xmax, ymax) =>
      Array(sfc.lon.normalize(xmin), sfc.lat.normalize(ymin), sfc.lon.normalize(xmax), sfc.lat.normalize(ymax))
    }.toArray

    (Z2HBaseFilter.Priority, new Z2HBaseFilter(new Z2Filter(normalizedXY, 8), offset))
  }
}

