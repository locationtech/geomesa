/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.filters.Z2HBaseFilter
import org.locationtech.geomesa.index.filters.Z2Filter
import org.locationtech.geomesa.index.index.{Z2Index, Z2ProcessingValues}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

case object HBaseZ2Index extends HBaseLikeZ2Index with HBasePlatform {
  override def configurePushDownFilters(config: HBaseFeatureIndex.ScanConfig, ecql: Option[Filter], transform: Option[(String, SimpleFeatureType)], sft: SimpleFeatureType): HBaseFeatureIndex.ScanConfig = {
    val z2Filter =
      org.locationtech.geomesa.index.index.Z2Index.currentProcessingValues match {
        case None                                           => Seq.empty[org.apache.hadoop.hbase.filter.Filter]
        case Some(Z2ProcessingValues(geoms, bounds))        => configureZ2PushDown(bounds)
      }

    val copy = super.configurePushDownFilters(config, ecql, transform, sft)
    copy.copy(hbaseFilters = copy.hbaseFilters ++ z2Filter)
  }

  private def configureZ2PushDown(xy: Seq[(Double, Double, Double, Double)]) = {
    val normalizedXY = xy.map { case (xmin, ymin, xmax, ymax) =>
      Array(Z2SFC.lon.normalize(xmin), Z2SFC.lat.normalize(ymin), Z2SFC.lon.normalize(xmax), Z2SFC.lat.normalize(ymax))
    }.toArray

    // TODO: deal with non-points in the XZ filter
    val filt = new Z2HBaseFilter(new Z2Filter(normalizedXY, 1, 8))
    Seq(filt)
  }
}

trait HBaseLikeZ2Index
    extends HBaseFeatureIndex with Z2Index[HBaseDataStore, HBaseFeature, Mutation, Query] {
  override val version: Int = 1
}
