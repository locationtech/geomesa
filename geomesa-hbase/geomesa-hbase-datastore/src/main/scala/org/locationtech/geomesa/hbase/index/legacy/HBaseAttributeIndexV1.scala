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
import org.locationtech.geomesa.hbase.index.{HBaseFeatureIndex, HBasePlatform}
import org.locationtech.geomesa.index.index.legacy.AttributeDateIndex

case object HBaseAttributeIndexV1 extends HBaseLikeAttributeIndexV1 with HBasePlatform

trait HBaseLikeAttributeIndexV1 extends HBaseFeatureIndex
    with AttributeDateIndex[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig] {
  override val version: Int = 1
}
