/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.core

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object GeoMesaServletCatalog extends LazyLogging {
  case class GeoMesaLayerInfo(ads: AccumuloDataStore, sft: SimpleFeatureType)

  private [this] val layers = new ConcurrentHashMap[(String, String), GeoMesaLayerInfo]()

  def getGeoMesaLayerInfo(workspace: String, layer: String) =
    Option(layers.get(workspace, layer))

  def putGeoMesaLayerInfo(workspace: String, layer: String, info: GeoMesaLayerInfo) =
    layers.put((workspace, layer), info)

  def removeGeoMesaLayerInfo(workspace: String, layer: String) =
    layers.remove((workspace, layer))

  def getKeys = layers.keysIterator
}