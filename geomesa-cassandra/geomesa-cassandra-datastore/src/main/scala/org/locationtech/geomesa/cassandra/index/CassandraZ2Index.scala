/***********************************************************************
 * Copyright (c) 2017-2018 IBM
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.cassandra.index

import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.cassandra.index.CassandraIndexAdapter.ScanConfig
import org.locationtech.geomesa.cassandra.{RowRange, RowValue}
import org.locationtech.geomesa.index.index.z2.Z2Index

case object CassandraZ2Index
    extends Z2Index[CassandraDataStore, CassandraFeature, Seq[RowValue], Seq[RowRange], ScanConfig]
    with CassandraFeatureIndex with CassandraZ2Layout with CassandraIndexAdapter  {
  override val version: Int = 2
}
