/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.index.index.XZ3Index

case object HBaseXZ3Index
    extends HBaseFeatureIndex with XZ3Index[HBaseDataStore, HBaseFeature, Mutation, Query] with HBasePlatform {
  override val version: Int = 1
}
