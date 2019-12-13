/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import java.awt.RenderingHints.Key
import java.io.Serializable
import java.util

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}

class GeoCQEngineDataStoreFactory extends DataStoreFactorySpi {
  override def createDataStore(params: util.Map[String, Serializable]): DataStore =
    if (GeoCQEngineDataStoreFactory.getUseGeoIndex(params))
      GeoCQEngineDataStore.engine
    else
      GeoCQEngineDataStore.engineNoGeoIndex

  override def createNewDataStore(params: util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def getDisplayName: String = "GeoCQEngine DataStore"

  override def getDescription: String = "GeoCQEngine DataStore"

  override def canProcess(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey("cqengine")
  }

  override def isAvailable: Boolean = true

  override def getParametersInfo: Array[Param] =
    GeoCQEngineDataStoreFactory.params

  override def getImplementationHints: util.Map[Key, _] = null
}

object GeoCQEngineDataStoreFactory {
  val UseGeoIndexKey = "useGeoIndex"
  val UseGeoIndexDefault = true
  val UseGeoIndexParam = new Param(
    UseGeoIndexKey,
    classOf[java.lang.Boolean],
    "Enable an index on the default geometry",
    false,
    UseGeoIndexDefault
  )
  val params = Array(
    UseGeoIndexParam
  )

  def getUseGeoIndex(params: util.Map[String, Serializable]): Boolean = {
    if (params.containsKey(UseGeoIndexKey))
      UseGeoIndexParam.lookUp(params).asInstanceOf[Boolean]
    else
      UseGeoIndexDefault
  }
}
