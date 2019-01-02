/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.geotools.data.{DataStore, DataStoreFinder}

/**
  * Caches accessing of DataStores.
  *
  * @param writeDataStoreParams
  */
case class DataStoreConnector(writeDataStoreParams: Map[String, String]) {
  @transient lazy val store: DataStore = DataStoreConnector.loadingMap.get(writeDataStoreParams)
}

object DataStoreConnector {
  import org.locationtech.geomesa.spark.CaseInsensitiveMapFix._

  val loadingMap = Caffeine.newBuilder().build[Map[String, String], DataStore](
    new CacheLoader[Map[String, String], DataStore] {
      override def load(key: Map[String, String]) = {
        DataStoreFinder.getDataStore(key)
      }
    })
}
