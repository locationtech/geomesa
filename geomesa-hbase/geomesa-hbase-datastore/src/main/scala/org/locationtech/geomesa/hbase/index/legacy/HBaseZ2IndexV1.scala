/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index.legacy

import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.hbase.index.{HBaseFeatureIndex, HBasePlatform, HBaseZ2PushDown}
import org.locationtech.geomesa.index.index.legacy.Z2LegacyIndex

case object HBaseZ2IndexV1 extends HBaseLikeZ2IndexV1 with HBasePlatform with HBaseZ2PushDown

trait HBaseLikeZ2IndexV1 extends HBaseFeatureIndex
    with Z2LegacyIndex[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig] {
  override val version: Int = 1
}
