/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

import java.io.Closeable

/**
 * GeoMesa Metadata/Catalog abstraction using key/value String pairs storing
 * them on a per-typeName basis
 */
trait GeoMesaMetadata[T] extends Closeable {

  /**
   * Returns existing simple feature types
   *
   * @return simple feature type names
   */
  def getFeatureTypes: Array[String]

  /**
   * Insert a value - any existing value under the given key will be overwritten
   *
   * @param typeName simple feature type name
   * @param key key
   * @param value value
   */
  def insert(typeName: String, key: String, value: T): Unit

  /**
   * Insert multiple values at once - may be more efficient than single inserts
   *
   * @param typeName simple feature type name
   * @param kvPairs key/values
   */
  def insert(typeName: String, kvPairs: Map[String, T]): Unit

  /**
    * Delete a key
    *
    * @param typeName simple feature type name
    * @param key key
    */
  def remove(typeName: String, key: String): Unit

  /**
    * Delete multiple keys at once - may be more efficient than single deletes
    *
    * @param typeName simple feature type name
    * @param keys keys
    */
  def remove(typeName: String, keys: Seq[String]): Unit

  /**
   * Reads a value
   *
   * @param typeName simple feature type name
   * @param key key
   * @param cache may return a cached value if true, otherwise may use a slower lookup
   * @return value, if present
   */
  def read(typeName: String, key: String, cache: Boolean = true): Option[T]

  /**
   * Reads a value. Throws an exception if value is missing
   *
   * @param typeName simple feature type name
   * @param key key
   * @return value
   */
  def readRequired(typeName: String, key: String): T =
    read(typeName, key).getOrElse {
      throw new RuntimeException(s"Unable to find required metadata property for $typeName:$key")
    }

  /**
    * Scan for keys starting with a given prefix
    *
    * @param typeName simple feature type name
    * @param prefix key prefix
    * @param cache may return a cached value if true, otherwise may use a slower lookup
    * @return keys -> values
    */
  def scan(typeName: String, prefix: String, cache: Boolean = true): Seq[(String, T)]

  /**
    * Invalidates any cached value for the given key
    *
    * @param typeName simple feature type name
    * @param key key
    */
  def invalidateCache(typeName: String, key: String): Unit

  /**
   * Deletes all values associated with a given feature type
   *
   * @param typeName simple feature type name
   */
  def delete(typeName: String)
}

object GeoMesaMetadata {

  // Metadata keys
  val ATTRIBUTES_KEY       = "attributes"
  val VERSION_KEY          = "version"

  val STATS_GENERATION_KEY = "stats-date"
  val STATS_INTERVAL_KEY   = "stats-interval"
}

trait HasGeoMesaMetadata[T] {
  def metadata: GeoMesaMetadata[T]
}


