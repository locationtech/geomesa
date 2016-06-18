/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.core

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.blob.core.GeoMesaBlobStoreSFT._

class GeoMesaAccumuloBlobStore(ds: AccumuloDataStore, bs: AccumuloBlobStoreImpl)
  extends GeoMesaGenericBlobStore(ds, bs) with LazyLogging

object GeoMesaAccumuloBlobStore {

  def apply(ds: AccumuloDataStore): GeoMesaAccumuloBlobStore = {
    val blobTableName = s"${ds.catalogTable}_blob"
    ds.createSchema(sft)

    val bwConf = GeoMesaBatchWriterConfig().setMaxWriteThreads(ds.config.writeThreads)

    val accBlobStore = new AccumuloBlobStoreImpl(ds.connector,
      blobTableName,
      ds.authProvider,
      ds.auditProvider,
      bwConf)

    new GeoMesaAccumuloBlobStore(ds, accBlobStore)
  }

}