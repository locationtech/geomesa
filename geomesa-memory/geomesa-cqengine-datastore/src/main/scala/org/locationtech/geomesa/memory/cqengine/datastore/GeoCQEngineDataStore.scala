package org.locationtech.geomesa.memory.cqengine.datastore

import java.util

import org.geotools.data.Query
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class GeoCQEngineDataStore extends ContentDataStore {
  val namesToEngine =  new java.util.concurrent.ConcurrentHashMap[String, GeoCQEngine]()

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
    namesToEngine.putIfAbsent(featureType.getTypeName, new GeoCQEngine(featureType))
  }
}

object GeoCQEngineDataStore {
  lazy val engine = new GeoCQEngineDataStore
}
