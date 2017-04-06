/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.bigtable.data

import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}

class BigtableDataStoreFactory extends HBaseDataStoreFactory {
  override def getDisplayName: String = BigtableDataStoreFactory.DisplayName
  override def getDescription: String = BigtableDataStoreFactory.Description

  override def buildConfig(catalog: String,
                           generateStats: Boolean,
                           audit: Option[(AuditWriter, AuditProvider, String)],
                           queryThreads: Int,
                           queryTimeout: Option[Long],
                           looseBBox: Boolean,
                           caching: Boolean,
                           authsProvider: Option[AuthorizationsProvider]) =
    HBaseDataStoreConfig(
      catalog,
      generateStats,
      audit,
      queryThreads,
      queryTimeout,
      looseBBox,
      caching,
      authsProvider,
      isBigtable = true
    )

}

object BigtableDataStoreFactory {
  val DisplayName = "Google Bigtable (GeoMesa)"
  val Description = "Google Bigtable\u2122 distributed key/value store"
}
