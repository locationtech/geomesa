/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.awt.RenderingHints
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.geotools.factory.FactoryIteratorProvider

/**
  * This class allows us to pass a concrete datastore instance through SPI loading, which is useful for testing
  * with e.g. a MemoryDataStore
  */
object DataStoreRegistration {

  val param = new Param("geomesa.test.ds")

  private val dataStores = new ConcurrentHashMap[String, DataStore]

  /**
    * Register a data store instance for SPI loading
    *
    * @param key unique parameter value to lookup the datastore with - e.g. Map("geomesa.test.ds" -> key)
    * @param ds data store instance
    */
  def register(key: String, ds: DataStore): Unit = synchronized {
    val add = dataStores.isEmpty
    dataStores.put(key, ds)
    if (add) {
      org.geotools.factory.GeoTools.addFactoryIteratorProvider(provider)
    }
  }

  /**
    * Unregister a data store instance for SPI loading
    *
    * @param key unique parameter value to lookup the datastore with - e.g. Map("geomesa.test.ds" -> key)
    * @param ds data store instance
    */
  def unregister(key: String, ds: DataStore): Unit = synchronized {
    require(ds.eq(dataStores.remove(key)), s"Multiple datastores registered under key $key")
    if (dataStores.isEmpty) {
      org.geotools.factory.GeoTools.removeFactoryIteratorProvider(provider)
    }
  }

  private val factory = new DataStoreFactorySpi() {

    override def createDataStore(params: java.util.Map[String, java.io.Serializable]): DataStore =
      dataStores.get(params.get(param.key).asInstanceOf[String])

    override def createNewDataStore(params: java.util.Map[String, java.io.Serializable]): DataStore =
      createDataStore(params)

    override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
      params.containsKey(param.key)

    override def getParametersInfo: Array[Param] = Array(param)

    override def getDisplayName: String = ""
    override def getDescription: String = ""
    override def isAvailable: Boolean = true
    override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
  }

  private val provider = new FactoryIteratorProvider() {
    override def iterator[T](category: Class[T]): java.util.Iterator[T] = {
      if (category != classOf[DataStoreFactorySpi]) { Collections.emptyIterator() } else {
        Collections.singleton(factory.asInstanceOf[T]).iterator
      }
    }
  }
}
