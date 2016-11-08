/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.bigtable.data

import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory

class BigtableDataStoreFactory extends HBaseDataStoreFactory {
  override def getDisplayName: String = BigtableDataStoreFactory.DisplayName
  override def getDescription: String = BigtableDataStoreFactory.Description
}

object BigtableDataStoreFactory {
  val DisplayName = "Google Bigtable (GeoMesa)"
  val Description = "Google Bigtable\u2122 distributed key/value store"
}
