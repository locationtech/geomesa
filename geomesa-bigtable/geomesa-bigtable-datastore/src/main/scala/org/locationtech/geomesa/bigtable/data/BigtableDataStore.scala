/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.data

import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.hbase.data._

class BigtableDataStore(connection: Connection, config: HBaseDataStoreConfig)
    extends HBaseDataStore(connection, config) {
  override val adapter: BigtableIndexAdapter = new BigtableIndexAdapter(this)
  override protected def loadIteratorVersions: Set[String] = Set.empty
}
