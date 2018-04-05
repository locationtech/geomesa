/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index.legacy

import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.{Mutation, Query}
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseFeature}
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.hbase.index.{HBaseFeatureIndex, HBaseIndexAdapter, HBasePlatform}
import org.locationtech.geomesa.index.index.legacy.AttributeShardedIndex

case object HBaseAttributeIndexV4 extends HBaseLikeAttributeIndexV4 with HBasePlatform

trait HBaseLikeAttributeIndexV4 extends HBaseFeatureIndex with HBaseIndexAdapter
    with AttributeShardedIndex[HBaseDataStore, HBaseFeature, Mutation, Query, ScanConfig] {

  override val version: Int = 4

  override def configureColumnFamilyDescriptor(desc: HColumnDescriptor): Unit = {
    desc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
  }
}
