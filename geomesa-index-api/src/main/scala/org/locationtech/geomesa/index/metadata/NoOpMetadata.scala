/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

class NoOpMetadata[T] extends GeoMesaMetadata[T] {

  override def getFeatureTypes: Array[String] = Array.empty

  override def insert(typeName: String, key: String, value: T): Unit = {}

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {}

  override def remove(typeName: String, key: String): Unit = {}

  override def remove(typeName: String, keys: Seq[String]): Unit = {}

  override def read(typeName: String, key: String, cache: Boolean): Option[T] = None

  override def scan(typeName: String, prefix: String, cache: Boolean): Seq[(String, T)] = Seq.empty

  override def invalidateCache(typeName: String, key: String): Unit = {}

  override def delete(typeName: String): Unit = {}

  override def close(): Unit = {}
}
