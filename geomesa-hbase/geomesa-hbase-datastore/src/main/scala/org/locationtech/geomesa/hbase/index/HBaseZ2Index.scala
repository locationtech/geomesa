/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.filters.Z2HBaseFilter
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.index.filters.Z2Filter
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexValues}
import org.opengis.feature.simple.SimpleFeatureType

case object HBaseZ2Index extends HBaseLikeZ2Index with HBasePlatform with HBaseZ2PushDown

trait HBaseLikeZ2Index extends HBaseFeatureIndex with HBaseIndexAdapter
    with Z2Index[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig] {
  override val version: Int = 2
}

trait HBaseZ2PushDown extends Z2Index[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig] {

  override protected def updateScanConfig(sft: SimpleFeatureType,
                                          config: ScanConfig,
                                          indexValues: Option[Z2IndexValues]): ScanConfig = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val z2Filter = indexValues.map { values =>
      val offset = if (sft.isTableSharing) { 2 } else { 1 } // sharing + shard - note: currently sharing is always false
      (Z2HBaseFilter.Priority, Z2HBaseFilter(Z2Filter(values), offset))
    }
    config.copy(filters = config.filters ++ z2Filter)
  }
}
