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
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.index.index.z3.XZ3Index

case object HBaseXZ3Index extends HBaseLikeXZ3Index with HBasePlatform

// TODO GEOMESA-1807 deal with non-points in a pushdown XZ filter
trait HBaseLikeXZ3Index extends HBaseFeatureIndex with HBaseIndexAdapter
    with XZ3Index[HBaseDataStore, HBaseFeature, Mutation, Scan, ScanConfig] {
  override val version: Int = 1
}
