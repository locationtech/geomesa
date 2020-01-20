/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import java.nio.charset.StandardCharsets

package object metadata {

  trait HasGeoMesaMetadata[T] {
    def metadata: GeoMesaMetadata[T]
  }

  trait MetadataSerializer[T] {
    def serialize(typeName: String, value: T): Array[Byte]
    def deserialize(typeName: String, value: Array[Byte]): T
  }

  object MetadataStringSerializer extends MetadataSerializer[String] {
    def serialize(typeName: String, value: String): Array[Byte] = {
      if (value == null) Array.empty else value.getBytes(StandardCharsets.UTF_8)
    }
    def deserialize(typeName: String, value: Array[Byte]): String = {
      if (value.isEmpty) null else new String(value, StandardCharsets.UTF_8)
    }
  }

  object NoOpMetadata extends NoOpMetadata[Any]

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
    override def backup(typeName: String): Unit = {}
    override def close(): Unit = {}
  }
}
