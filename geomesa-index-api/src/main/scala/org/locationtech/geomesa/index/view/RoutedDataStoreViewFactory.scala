/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.config._
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStore, DataStoreFactorySpi, DataStoreFinder}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreInfo, NamespaceParams}
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.ReadWriteFlag

import java.awt.RenderingHints
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Data store factory for routed views
  */
class RoutedDataStoreViewFactory extends DataStoreFactorySpi {

  import RoutedDataStoreViewFactory._

  import scala.collection.JavaConverters._

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    RoutedDataStoreViewFactory.canProcess(params)

  override def createDataStore(params: java.util.Map[String, _]): DataStore =
    createNewDataStore(params)

<<<<<<< HEAD
  override def createNewDataStore(params: java.util.Map[String, _]): DataStore = {
=======
  override def createNewDataStore(params: java.util.Map[String, java.io.Serializable]): DataStore = {
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
    val config = ConfigFactory.parseString(ConfigParam.lookup(params)).resolve()
    val configs = if (config.hasPath("stores")) { config.getConfigList("stores").asScala } else { Seq.empty }
    if (configs.isEmpty) {
      throw new IllegalArgumentException("No 'stores' element defined in configuration")
    }

    val namespace = NamespaceParam.lookupOpt(params)
    val nsConfig = namespace.map(ConfigValueFactory.fromAnyRef)

    val stores = Seq.newBuilder[(DataStore, java.util.Map[String, _ <: AnyRef])]
    stores.sizeHint(configs.length)

    try {
      configs.foreach { config =>
        lazy val error = new IllegalArgumentException(s"Could not load store using configuration:\n" +
            config.root().render(ConfigRenderOptions.concise().setFormatted(true)))
        // inject the namespace into the underlying stores
        val storeParams = nsConfig.map(config.withValue(NamespaceParam.key, _)).getOrElse(config).root().unwrapped()
        Try(DataStoreFinder.getDataStore(storeParams)) match {
          case Success(null)  => throw error
          case Success(store) => stores += store -> storeParams
          case Failure(e)     => throw error.initCause(e)
        }
      }
    } catch {
      case NonFatal(e) => stores.result.foreach(_._1.dispose()); throw e
    }

    val storesAndConfigs = stores.result

    try {
      val router = {
        val impl = RouterParam.lookup(params)
        ServiceLoader.load[RouteSelector]().find(_.getClass.getName == impl).getOrElse {
          throw new IllegalArgumentException(s"Could not load route selector '$impl'. Available implementations: " +
              ServiceLoader.load[RouteSelector]().map(_.getClass.getName).mkString(", "))
        }
      }

      router.init(storesAndConfigs)

      new RoutedDataStoreView(storesAndConfigs.map(_._1), router, namespace)
    } catch {
      case NonFatal(e) => storesAndConfigs.foreach(_._1.dispose()); throw e
    }
  }

  override def getDisplayName: String = DisplayName

  override def getDescription: String = Description

  override def getParametersInfo: Array[Param] = Array(ParameterInfo :+ NamespaceParam: _*)

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object RoutedDataStoreViewFactory extends GeoMesaDataStoreInfo with NamespaceParams {

  override val DisplayName: String = "Routed DataStore View (GeoMesa)"
  override val Description: String = "A routable, read-only view of multiple data stores"

  val ConfigParam =
    new GeoMesaParam[String](
      "geomesa.routed.stores",
      "Typesafe configuration defining the underlying data stores to query",
      optional = false,
      largeText = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val RouterParam =
    new GeoMesaParam[String](
      "geomesa.route.selector",
      "Class name for a custom org.locationtech.geomesa.index.view.RouteSelector implementation",
      default = classOf[RouteSelectorByAttribute].getName,
      enumerations = ServiceLoader.load[RouteSelector]().map(_.getClass.getName),
      readWrite = ReadWriteFlag.ReadOnly
    )

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] = Array(ConfigParam, RouterParam)

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    params.containsKey(ConfigParam.key)
}
