/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.data

import java.awt.RenderingHints.Key
import java.io.Serializable
import java.net.URL
import java.util.Collections

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, FileDataStore, FileDataStoreFactorySpi, Parameter}

import scala.util.Try

class ArrowDataStoreFactory extends FileDataStoreFactorySpi {
  import ArrowDataStoreFactory._

  // FileDataStoreFactory methods

  override def createDataStore(url: URL): FileDataStore = new ArrowDataStore(url)

  override def getTypeName(url: URL): String = new ArrowDataStore(url).getSchema().getTypeName

  override def getFileExtensions: Array[String] = Array("arrow")

  override def canProcess(url: URL): Boolean = url != null && url.getFile.endsWith(".arrow")

  // DataStoreFactory methods

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    Option(UrlParam.lookUp(params).asInstanceOf[URL]).map(createDataStore).getOrElse {
      throw new IllegalArgumentException(s"Could not create data store using $params")
    }
  }

  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    Try(Option(UrlParam.lookUp(params).asInstanceOf[URL]).exists(canProcess)).getOrElse(false)

  override def getParametersInfo: Array[Param] = Array(UrlParam)

  override def getDisplayName: String = DisplayName

  override def getDescription: String = Description

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[Key, _] =
    java.util.Collections.EMPTY_MAP.asInstanceOf[java.util.Map[Key, _]]
}

object ArrowDataStoreFactory {
  val UrlParam = new Param("url", classOf[URL], "url to an arrow file", true, null, Collections.singletonMap(Parameter.EXT, "arrow"))

  private val DisplayName = "Apache Arrow (GeoMesa)"

  private val Description = "Arrow file-based data store"
}
