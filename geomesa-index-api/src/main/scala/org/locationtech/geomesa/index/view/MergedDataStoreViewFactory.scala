/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import java.awt.RenderingHints

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, DataStoreFinder, Parameter}

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
    val configs = {
      val config = ConfigFactory.parseString(ConfigParam.lookUp(params).asInstanceOf[String])
      if (config.hasPath("stores")) { config.getConfigList("stores") } else {
        throw new IllegalArgumentException("No 'stores' element defined in configuration")
      }
    }

    val stores = Seq.newBuilder[DataStore]
    stores.sizeHint(configs.size())

    try {
      configs.asScala.foreach { config =>
        lazy val error = new IllegalArgumentException(s"Could not load store using configuration:\n" +
            config.root().render(ConfigRenderOptions.concise().setFormatted(true)))
        Try(DataStoreFinder.getDataStore(config.root().unwrapped())) match {
          case Success(null)  => throw error
          case Success(store) => stores += store
          case Failure(e)     => throw error.initCause(e)
        }
      }
    } catch {
      case NonFatal(e) => stores.result.foreach(_.dispose()); throw e
    }

    new MergedDataStoreView(stores.result, None)
  }

  override def getDisplayName: String = DisplayName

  override def getDescription: String = Description

  override def getParametersInfo: Array[Param] = Array(ConfigParam)

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object MergedDataStoreViewFactory {

  val DisplayName: String = "Merged DataStore View (GeoMesa)"
  val Description: String = "A merged, read-only view of multiple data stores"

  val ConfigParam = new Param("geomesa.merged.stores", classOf[String], "Typesafe configuration defining the underlying data stores to query", true, null, java.util.Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))

  def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    params.containsKey(ConfigParam.key)
}
