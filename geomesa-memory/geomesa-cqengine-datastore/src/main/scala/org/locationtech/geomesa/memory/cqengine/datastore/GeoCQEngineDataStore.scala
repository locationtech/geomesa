/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.Query
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType

import java.util
import scala.collection.JavaConverters._

class GeoCQEngineDataStore(useGeoIndex: Boolean) extends ContentDataStore with LazyLogging {

  logger.debug(s"useGeoIndex=$useGeoIndex")

  val namesToEngine = new java.util.concurrent.ConcurrentHashMap[String, GeoCQEngine]()

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val engine = namesToEngine.get(entry.getTypeName)
    if (engine != null) {
      new GeoCQEngineFeatureStore(engine, entry, Query.ALL)
    } else {
      null
    }
  }

  override def createTypeNames(): util.List[Name] = { namesToEngine.keys().asScala.toList.map { new NameImpl(_).asInstanceOf[Name] }.asJava }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    val geo = if (!useGeoIndex) { Seq.empty } else {
      Seq((featureType.getGeometryDescriptor.getLocalName, CQIndexType.GEOMETRY))
    }
    val attributes = CQIndexType.getDefinedAttributes(featureType) ++ geo
    namesToEngine.putIfAbsent(featureType.getTypeName, new GeoCQEngine(featureType, attributes))
  }
}

object GeoCQEngineDataStore {

  private val stores = Caffeine.newBuilder().build[(String, Boolean), GeoCQEngineDataStore](
    new CacheLoader[(String, Boolean), GeoCQEngineDataStore]() {
      override def load(key: (String, Boolean)): GeoCQEngineDataStore = new GeoCQEngineDataStore(useGeoIndex = key._2)
    }
  )

  def getStore(useGeoIndex: Boolean = true, namespace: Option[String] = None): GeoCQEngineDataStore =
    stores.get((namespace.getOrElse(""), useGeoIndex))
}
