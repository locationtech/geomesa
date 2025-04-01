/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineDataStoreFactory.{CqEngineParam, NamespaceParam}

import java.awt.RenderingHints.Key
import java.util

class GeoCQEngineDataStoreFactory extends DataStoreFactorySpi {

  override def createDataStore(params: java.util.Map[String, _]): DataStore = {
    val namespace = Option(NamespaceParam.lookUp(params).asInstanceOf[String])
    GeoCQEngineDataStore.getStore(GeoCQEngineDataStoreFactory.getUseGeoIndex(params), namespace)
  }

  override def createNewDataStore(params: java.util.Map[String, _]): DataStore = createDataStore(params)

  override def getDisplayName: String = "GeoCQEngine DataStore"

  override def getDescription: String = "GeoCQEngine DataStore"

  override def canProcess(params: util.Map[String, _]): Boolean =
    params.containsKey(CqEngineParam.key) && CqEngineParam.lookUp(params).asInstanceOf[java.lang.Boolean].booleanValue()

  override def isAvailable: Boolean = true

  override def getParametersInfo: Array[Param] =
    GeoCQEngineDataStoreFactory.params

  override def getImplementationHints: util.Map[Key, _] = null
}

object GeoCQEngineDataStoreFactory {

  private val UseGeoIndexKey = "useGeoIndex"
  private val UseGeoIndexDefault = true

  val CqEngineParam: Param = new Param(
    "cqengine",
    classOf[java.lang.Boolean],
    "Use the CQEngine data store",
    true
  )

  val UseGeoIndexParam: Param = new Param(
    UseGeoIndexKey,
    classOf[java.lang.Boolean],
    "Enable an index on the default geometry",
    false,
    UseGeoIndexDefault
  )

  val NamespaceParam: Param = new Param("namespace", classOf[String], "Namespace", false)

  val params: Array[Param] = Array(
    CqEngineParam,
    UseGeoIndexParam,
    NamespaceParam,
  )

  def getUseGeoIndex(params: java.util.Map[String, _]): Boolean = {
    if (params.containsKey(UseGeoIndexKey)) {
      UseGeoIndexParam.lookUp(params).asInstanceOf[Boolean]
    } else {
      UseGeoIndexDefault
    }
  }
}
