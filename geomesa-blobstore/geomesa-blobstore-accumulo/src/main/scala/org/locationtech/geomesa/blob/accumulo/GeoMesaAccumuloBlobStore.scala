/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.blob.api.GeoMesaGenericBlobStore

class GeoMesaAccumuloBlobStore(ds: AccumuloDataStore, bs: AccumuloBlobStoreImpl)
  extends GeoMesaGenericBlobStore(ds, bs) with LazyLogging

object GeoMesaAccumuloBlobStore {

  def apply(ds: AccumuloDataStore): GeoMesaAccumuloBlobStore = {
    val blobTableName = s"${ds.config.catalog}${AccumuloBlobStoreImpl.blobSuffix}"

    val bwConf = GeoMesaBatchWriterConfig().setMaxWriteThreads(ds.config.writeThreads)

    val accBlobStore = new AccumuloBlobStoreImpl(ds.connector,
      blobTableName,
      ds.config.authProvider,
      ds.config.audit.get._2,
      bwConf)

    new GeoMesaAccumuloBlobStore(ds, accBlobStore)
  }

}