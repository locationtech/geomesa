/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import java.awt.RenderingHints

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, DataStoreFinder}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreInfo, NamespaceParams}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Data store factory for merged view
  */
class MergedDataStoreViewFactory extends DataStoreFactorySpi {

  import MergedDataStoreViewFactory._

  import scala.collection.JavaConverters._

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    MergedDataStoreViewFactory.canProcess(params)

  override def createDataStore(params: java.util.Map[String, java.io.Serializable]): DataStore =
    createNewDataStore(params)

  override def createNewDataStore(params: java.util.Map[String, java.io.Serializable]): DataStore = {
    val namespace = NamespaceParam.lookupOpt(params)

    val configs = {
      val config = ConfigFactory.parseString(ConfigParam.lookup(params))
      if (config.hasPath("stores")) { config.getConfigList("stores") } else {
        throw new IllegalArgumentException("No 'stores' element defined in configuration")
      }
    }
    val nsConfig = namespace.map(ConfigValueFactory.fromAnyRef)

    val stores = Seq.newBuilder[DataStore]
    stores.sizeHint(configs.size())

    try {
      configs.asScala.foreach { config =>
        lazy val error = new IllegalArgumentException(s"Could not load store using configuration:\n" +
            config.root().render(ConfigRenderOptions.concise().setFormatted(true)))
        // inject the namespace into the underlying stores
        val storeParams = nsConfig.map(config.withValue(NamespaceParam.key, _)).getOrElse(config).root().unwrapped()
        Try(DataStoreFinder.getDataStore(storeParams)) match {
          case Success(null)  => throw error
          case Success(store) => stores += store
          case Failure(e)     => throw error.initCause(e)
        }
      }
    } catch {
      case NonFatal(e) => stores.result.foreach(_.dispose()); throw e
    }

    new MergedDataStoreView(stores.result, namespace)
  }

  override def getDisplayName: String = DisplayName

  override def getDescription: String = Description

  override def getParametersInfo: Array[Param] = ParameterInfo :+ NamespaceParam

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object MergedDataStoreViewFactory extends GeoMesaDataStoreInfo with NamespaceParams {

  override val DisplayName: String = "Merged DataStore View (GeoMesa)"
  override val Description: String = "A merged, read-only view of multiple data stores"

  val ConfigParam = new GeoMesaParam[String]("geomesa.merged.stores", "Typesafe configuration defining the underlying data stores to query", optional = false, largeText = true)

  override val ParameterInfo: Array[GeoMesaParam[_]] = Array(ConfigParam)

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    params.containsKey(ConfigParam.key)
}
