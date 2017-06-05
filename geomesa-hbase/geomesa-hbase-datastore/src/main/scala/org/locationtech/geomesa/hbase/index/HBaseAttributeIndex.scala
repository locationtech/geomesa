/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.index.index.{AttributeDateIndex, AttributeIndex}
import org.locationtech.geomesa.index.utils.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType

case object HBaseAttributeIndex extends HBaseAttributeLikeIndex with HBasePlatform {
  override val version: Int = 3
}

// no shards
case object HBaseAttributeIndexV2 extends HBaseAttributeLikeIndex with HBasePlatform {
  override val version: Int = 2

  override protected def getShards(sft: SimpleFeatureType): IndexedSeq[Array[Byte]] = SplitArrays.EmptySplits
}

trait HBaseAttributeLikeIndex
    extends HBaseFeatureIndex with AttributeIndex[HBaseDataStore, HBaseFeature, Mutation, Query]

trait HBaseAttributeDateLikeIndex
    extends HBaseFeatureIndex with AttributeDateIndex[HBaseDataStore, HBaseFeature, Mutation, Query] {
  override val version: Int = 1
}

case object HBaseAttributeDateIndex extends HBaseAttributeDateLikeIndex with HBasePlatform