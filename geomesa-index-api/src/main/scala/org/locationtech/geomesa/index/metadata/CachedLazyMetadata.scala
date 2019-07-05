/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{Cache, CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.collection.{CloseableIterator, IsSynchronized, MaybeSynchronized, NotSynchronized}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.WithClose

/**
  * Backs metadata with a cache to save repeated database reads. Underlying table will be lazily created
  * when required.
  *
  * @tparam T type param
  */
trait CachedLazyMetadata[T] extends GeoMesaMetadata[T] with LazyLogging {

  import scala.collection.JavaConverters._

  protected def serializer: MetadataSerializer[T]

  /**
    * Checks if the underlying table exists
    *
    * @return
    */
  protected def checkIfTableExists: Boolean

  /**
    * Creates the underlying table
    */
  protected def createTable(): Unit

  /**
    * Writes key/value pairs
    *
    * @param rows keys/values
    */
  protected def write(typeName: String, rows: Seq[(String, Array[Byte])]): Unit

  /**
    * Deletes multiple rows
    *
    * @param typeName simple feature type name
    * @param keys keys
    */
  protected def delete(typeName: String, keys: Seq[String])

  /**
    * Reads a value from the underlying table
    *
    * @param typeName simple feature type name
    * @param key key
    * @return value, if it exists
    */
  protected def scanValue(typeName: String, key: String): Option[Array[Byte]]

  /**
    * Reads row keys from the underlying table
    *
    * @param typeName simple feature type name
    * @param prefix scan prefix, or empty string for all values
    * @return matching tuples of (key, value)
    */
  protected def scanValues(typeName: String, prefix: String = ""): CloseableIterator[(String, Array[Byte])]

  /**
    * Reads all row keys from the underlying table
    *
    * @return matching tuples of (typeName, key)
    */
  protected def scanKeys(): CloseableIterator[(String, String)]

  // only synchronize if table doesn't exist - otherwise it's ready only and we can avoid synchronization
  private val tableExists: MaybeSynchronized[Boolean] =
    if (checkIfTableExists) { new NotSynchronized(true) } else { new IsSynchronized(false) }

  private val expiry = CachedLazyMetadata.Expiry.toDuration.get.toMillis

  // cache for our metadata - invalidate every 10 minutes so we keep things current
  private val metaDataCache =
    Caffeine.newBuilder().expireAfterWrite(expiry, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(String, String), Option[T]] {
        override def load(typeNameAndKey: (String, String)): Option[T] = {
          if (!tableExists.get) { None } else {
            val (typeName, key) = typeNameAndKey
            scanValue(typeName, key).map(serializer.deserialize(typeName, _))
          }
        }
      }
    )

  // keep a separate cache for scan queries vs point lookups, so that the point lookups don't cache
  // partial values for a scan result
  private val metaDataScanCache =
    Caffeine.newBuilder().expireAfterWrite(expiry, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(String, String), Seq[(String, T)]] {
        override def load(typeNameAndPrefix: (String, String)): Seq[(String, T)] = {
          if (!tableExists.get) { Seq.empty } else {
            val (typeName, prefix) = typeNameAndPrefix
            WithClose(scanValues(typeName, prefix)) { iter =>
              val builder = Seq.newBuilder[(String, T)]
              iter.foreach { case (k, v) => builder += k -> serializer.deserialize(typeName, v) }
              builder.result()
            }
          }
        }
      }
    )

  override def getFeatureTypes: Array[String] = {
    if (!tableExists.get) { Array.empty } else {
      WithClose(scanKeys()) { keys =>
        keys.collect { case (typeName, key) if key == GeoMesaMetadata.ATTRIBUTES_KEY => typeName }.toArray
      }
    }
  }

  override def read(typeName: String, key: String, cache: Boolean): Option[T] = {
    if (!cache) {
      metaDataCache.invalidate((typeName, key))
    }
    metaDataCache.get((typeName, key))
  }

  override def scan(typeName: String, prefix: String, cache: Boolean): Seq[(String, T)] = {
    if (!cache) {
      metaDataScanCache.invalidate((typeName, prefix))
    }
    metaDataScanCache.get((typeName, prefix))
  }

  override def insert(typeName: String, key: String, value: T): Unit = insert(typeName, Map(key -> value))

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {
    ensureTableExists()
    val strings = kvPairs.map { case (k, v) =>
      metaDataCache.put((typeName, k), Option(v)) // note: side effect in map
      (k, serializer.serialize(typeName, v))
    }
    write(typeName, strings.toSeq)
    invalidate(metaDataScanCache, typeName)
  }

  override def invalidateCache(typeName: String, key: String): Unit = {
    metaDataCache.invalidate((typeName, key))
    invalidate(metaDataScanCache, typeName)
  }

  override def remove(typeName: String, key: String): Unit = remove(typeName, Seq(key))

  override def remove(typeName: String, keys: Seq[String]): Unit = {
    if (tableExists.get) {
      delete(typeName, keys)
      // also remove from the cache
      keys.foreach(k => metaDataCache.invalidate((typeName, k)))
      invalidate(metaDataScanCache, typeName)
    } else {
      logger.debug(s"Trying to delete '$typeName: ${keys.mkString(", ")}' but table does not exist")
    }
  }

  override def delete(typeName: String): Unit = {
    if (tableExists.get) {
      WithClose(scanValues(typeName)) { keys =>
        if (keys.nonEmpty) {
          delete(typeName, keys.map(_._1).toSeq)
        }
      }
    } else {
      logger.debug(s"Trying to delete type '$typeName' but table does not exist")
    }
    Seq(metaDataCache, metaDataScanCache).foreach(invalidate(_, typeName))
  }

  // checks that the table is already created, and creates it if not
  def ensureTableExists(): Unit = tableExists.set(true, false, createTable())

  /**
    * Invalidate all keys for the given feature type
    *
    * @param cache cache to invalidate
    * @param typeName feature type name
    */
  private def invalidate(cache: Cache[(String, String), _], typeName: String): Unit = {
    cache.asMap.asScala.keys.foreach { k =>
      if (k._1 == typeName) {
        cache.invalidate(k)
      }
    }
  }
}

object CachedLazyMetadata {
  val Expiry = SystemProperty("geomesa.metadata.expiry", "10 minutes")
}
