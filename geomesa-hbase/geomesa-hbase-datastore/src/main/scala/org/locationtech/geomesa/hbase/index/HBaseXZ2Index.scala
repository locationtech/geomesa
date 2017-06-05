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
import org.locationtech.geomesa.index.index.XZ2Index

case object HBaseXZ2Index
    extends HBaseXZ2LikeIndex with HBasePlatform

trait HBaseXZ2LikeIndex
  extends HBaseFeatureIndex with XZ2Index[HBaseDataStore, HBaseFeature, Mutation, Query] {
  override val version: Int = 1

  // TODO GEOMESA-1807 deal with non-points in a pushdown XZ filter
}
