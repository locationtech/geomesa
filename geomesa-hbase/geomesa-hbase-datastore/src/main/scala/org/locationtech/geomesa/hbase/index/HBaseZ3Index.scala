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
import org.locationtech.geomesa.hbase.filters.Z3HBaseFilter
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.index.index.z3.{Z3Index, Z3IndexValues}
import org.opengis.feature.simple.SimpleFeatureType

case object HBaseZ3Index extends HBaseLikeZ3Index with HBasePlatform with HBaseZ3PushDown

trait HBaseLikeZ3Index extends HBaseFeatureIndex with HBaseIndexAdapter
    with Z3Index[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig] {
  override val version: Int = 2
}

trait HBaseZ3PushDown extends Z3Index[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig] {

  override protected def updateScanConfig(sft: SimpleFeatureType,
                                          config: ScanConfig,
                                          indexValues: Option[Z3IndexValues]): ScanConfig = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val z3Filter = indexValues.map { values =>
      val offset = if (sft.isTableSharing) { 2 } else { 1 } // sharing + shard - note: currently sharing is always false
      (Z3HBaseFilter.Priority, Z3HBaseFilter(Z3Filter(values), offset))
    }
    config.copy(filters = config.filters ++ z3Filter)
  }
}
