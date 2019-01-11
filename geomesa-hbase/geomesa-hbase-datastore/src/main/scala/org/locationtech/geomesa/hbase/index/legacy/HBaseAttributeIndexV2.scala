/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index.legacy

import org.apache.hadoop.hbase.client.{Mutation, Scan}
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseFeature}
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.hbase.index.{HBaseFeatureIndex, HBasePlatform}
import org.locationtech.geomesa.index.index.legacy.AttributeZIndex
import org.locationtech.geomesa.index.utils.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType

case object HBaseAttributeIndexV2 extends HBaseLikeAttributeIndexV2 with HBasePlatform

// no shards
trait HBaseLikeAttributeIndexV2 extends HBaseFeatureIndex
    with AttributeZIndex[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig] {
  override val version: Int = 2
  override protected def getShards(sft: SimpleFeatureType): IndexedSeq[Array[Byte]] = SplitArrays.EmptySplits
}
