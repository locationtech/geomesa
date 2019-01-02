/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class GeoCQEngineDataStore(useGeoIndex: Boolean) extends ContentDataStore with LazyLogging {

  logger.info(s"useGeoIndex=$useGeoIndex")

  val namesToEngine = new java.util.concurrent.ConcurrentHashMap[String, GeoCQEngine]()

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val engine = namesToEngine.get(entry.getTypeName)
    if (engine != null) {
      new GeoCQEngineFeatureStore(engine, entry, Query.ALL)
    } else {
      null
    }
  }

  override def createTypeNames(): util.List[Name] = { namesToEngine.keys().toList.map { new NameImpl(_) } }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    val geo = if (!useGeoIndex) { Seq.empty } else {
      Seq((featureType.getGeometryDescriptor.getLocalName, CQIndexType.GEOMETRY))
    }
    val attributes = CQIndexType.getDefinedAttributes(featureType) ++ geo
    namesToEngine.putIfAbsent(featureType.getTypeName, new GeoCQEngine(featureType, attributes))
  }
}

object GeoCQEngineDataStore {
  lazy val engine = new GeoCQEngineDataStore(useGeoIndex = true)
  lazy val engineNoGeoIndex = new GeoCQEngineDataStore(useGeoIndex = false)
}
