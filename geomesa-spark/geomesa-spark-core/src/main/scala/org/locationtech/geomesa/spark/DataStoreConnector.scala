/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark

import org.geotools.data.{DataStore, DataStoreFinder}

/**
  * Caches accessing of DataStores.
  * @param writeDataStoreParams
  */
case class DataStoreConnector(writeDataStoreParams: Map[String, String]) {
  @transient lazy val store: DataStore = DataStoreConnector.getOrFindDataStore(this.writeDataStoreParams)
}

object DataStoreConnector {
  import org.locationtech.geomesa.spark.CaseInsensitiveMapFix._

  private val writeLock = new Object
  @volatile private var connectionMap: Map[Map[String, String], DataStore] = Map()

  def getOrFindDataStore(writeDataStoreParams: Map[String, String]): DataStore = {
    var map = connectionMap
    map.get(writeDataStoreParams) match {
      case None =>
        writeLock.synchronized {
          map = connectionMap
          map.get(writeDataStoreParams) match {
            case None =>
              val ds = DataStoreFinder.getDataStore(writeDataStoreParams)
              connectionMap += writeDataStoreParams -> ds
              ds
            case Some(conn) =>
              conn
          }
        }
      case Some(conn) =>
        conn
    }
  }
}
