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
  */
object DataStoreConnector {

  import scala.collection.JavaConverters._

  def apply[T <: DataStore](params: Map[String, Object]): T = loadingMap.get(params).asInstanceOf[T]

  private val loadingMap = Caffeine.newBuilder().build[Map[String, Object], DataStore](
    new CacheLoader[Map[String, Object], DataStore] {
      override def load(key: Map[String, Object]): DataStore = DataStoreFinder.getDataStore(key.asJava)
    }
  )
}
