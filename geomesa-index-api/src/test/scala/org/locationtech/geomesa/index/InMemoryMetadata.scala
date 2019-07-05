/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import org.locationtech.geomesa.index.metadata.GeoMesaMetadata

class InMemoryMetadata[T] extends GeoMesaMetadata[T] {

  import scala.collection.mutable.{ Map => mMap }

  private val schemas = mMap.empty[String, mMap[String, T]]

  override def getFeatureTypes: Array[String] = synchronized(schemas.keys.toArray)

  override def insert(typeName: String, key: String, value: T): Unit = synchronized {
    schemas.getOrElseUpdate(typeName, mMap.empty[String, T]).put(key, value)
  }

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = synchronized {
    val m = schemas.getOrElseUpdate(typeName, mMap.empty[String, T])
    kvPairs.foreach { case (k, v) => m.put(k, v) }
  }

  override def remove(typeName: String, key: String): Unit = synchronized {
    schemas.get(typeName).foreach(_.remove(key))
  }

  override def remove(typeName: String, keys: Seq[String]): Unit = keys.foreach(remove(typeName, _))

  override def read(typeName: String, key: String, cache: Boolean): Option[T] = synchronized {
    schemas.get(typeName).flatMap(_.get(key))
  }

  override def scan(typeName: String, prefix: String, cache: Boolean): Seq[(String, T)] = synchronized {
    schemas.get(typeName) match {
      case None => Seq.empty
      case Some(m) => m.filterKeys(_.startsWith(prefix)).toSeq
    }
  }

  override def delete(typeName: String): Unit = synchronized {
    schemas.remove(typeName)
  }

  override def invalidateCache(typeName: String, key: String): Unit = {}

  override def close(): Unit = {}
}
