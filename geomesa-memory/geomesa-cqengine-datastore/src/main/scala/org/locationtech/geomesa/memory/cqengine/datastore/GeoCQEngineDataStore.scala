/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{Query, Transaction}
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureStore}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType

import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class GeoCQEngineDataStore(useGeoIndex: Boolean, namesToEngine: ConcurrentHashMap[String, GeoCQEngine])
    extends ContentDataStore with LazyLogging {

  logger.debug(s"useGeoIndex=$useGeoIndex")

  override def getFeatureSource(typeName: String, tx: Transaction): ContentFeatureStore =
    super.getFeatureSource(typeName, tx).asInstanceOf[ContentFeatureStore]
  override def getFeatureSource(typeName: Name, tx: Transaction): ContentFeatureStore =
    super.getFeatureSource(typeName, tx).asInstanceOf[ContentFeatureStore]
  override def getFeatureSource(typeName: Name): ContentFeatureStore =
    super.getFeatureSource(typeName).asInstanceOf[ContentFeatureStore]
  override def getFeatureSource(typeName: String): ContentFeatureStore =
    super.getFeatureSource(typeName).asInstanceOf[ContentFeatureStore]

  override def createFeatureSource(entry: ContentEntry): ContentFeatureStore = {
    val engine = namesToEngine.get(entry.getTypeName)
    if (engine != null) {
      new GeoCQEngineFeatureStore(engine, entry, Query.ALL)
    } else {
      null
    }
  }

  override def createTypeNames(): util.List[Name] = { namesToEngine.keys().asScala.toList.map { new NameImpl(_).asInstanceOf[Name] }.asJava }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    namesToEngine.computeIfAbsent(featureType.getTypeName, _ => {
      val geo = if (!useGeoIndex) { Seq.empty } else {
        Seq((featureType.getGeometryDescriptor.getLocalName, CQIndexType.GEOMETRY))
      }
      val attributes = CQIndexType.getDefinedAttributes(featureType) ++ geo
      new GeoCQEngine(featureType, attributes)
    })
  }
}

object GeoCQEngineDataStore {

  private val engines = Caffeine.newBuilder().build[(String, Boolean), ConcurrentHashMap[String, GeoCQEngine]](
    new CacheLoader[(String, Boolean), ConcurrentHashMap[String, GeoCQEngine]]() {
      override def load(key: (String, Boolean)): ConcurrentHashMap[String, GeoCQEngine] =
        new ConcurrentHashMap[String, GeoCQEngine]()
    }
  )

  def getStore(useGeoIndex: Boolean = true, namespace: Option[String] = None): GeoCQEngineDataStore =
    new GeoCQEngineDataStore(useGeoIndex, engines.get((namespace.getOrElse(""), useGeoIndex)))
}
