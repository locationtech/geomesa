/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index.legacy

import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.index.{HBaseFeatureIndex, HBasePlatform, HBaseZ3PushDown}
import org.locationtech.geomesa.index.index.legacy.Z3LegacyIndex
import org.locationtech.geomesa.index.index.z3.Z3IndexValues

case object HBaseZ3IndexV1 extends HBaseLikeZ3IndexV1 with HBasePlatform[Z3IndexValues]

trait HBaseLikeZ3IndexV1 extends HBaseFeatureIndex with HBaseZ3PushDown
    with Z3LegacyIndex[HBaseDataStore, HBaseFeature, Mutation, Query]  {
  override val version: Int = 1
}
