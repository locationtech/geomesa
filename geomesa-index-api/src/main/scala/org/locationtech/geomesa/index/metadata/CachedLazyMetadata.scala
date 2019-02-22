/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.index.metadata.CachedLazyMetadata.ScanQuery
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
    * @param query row key prefix
    * @return matching tuples of (typeName, key, value)
    */
  protected def scanValues(query: Option[ScanQuery]): CloseableIterator[(String, String, Array[Byte])]

  // only synchronize if table doesn't exist - otherwise it's ready only and we can avoid synchronization
  private val tableExists: MaybeSynchronized[Boolean] =
    if (checkIfTableExists) { new NotSynchronized(true) } else { new IsSynchronized(false) }

  // cache for our metadata - invalidate every 10 minutes so we keep things current
  private val metaDataCache = {
    val expiry = CachedLazyMetadata.Expiry.toDuration.get.toMillis
    Caffeine.newBuilder().expireAfterWrite(expiry, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(String, String), Option[T]] {
        override def load(k: (String, String)): Option[T] = {
          if (!tableExists.get) { None } else {
            scanValue(k._1, k._2).map(serializer.deserialize(k._1, _))
          }
        }
      }
    )
  }

  override def getFeatureTypes: Array[String] = {
    if (!tableExists.get) { Array.empty } else {
      WithClose(scanValues(None)) { keys =>
        keys.collect { case (typeName, key, _) if key == GeoMesaMetadata.ATTRIBUTES_KEY => typeName }.toArray
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
    import scala.collection.JavaConverters._

    def noCache: Seq[(String, T)] = {
      WithClose(scanValues(Some(ScanQuery(typeName, Option(prefix))))) { iter =>
        iter.map { case (t, k, v) =>
          val value = serializer.deserialize(t, v)
          metaDataCache.put((t, k), Option(value))
          k -> value
        }.toSeq
      }
    }

    if (!tableExists.get) {
      Seq.empty
    } else if (cache) {
      val result = metaDataCache.asMap().asScala.flatMap { case ((t, k), value) =>
        if (t == typeName && k.startsWith(prefix)) {
          value.map(v => k -> v)
        } else {
          Seq.empty
        }
      }
      if (result.nonEmpty) { result.toSeq } else { noCache }
    } else {
      noCache
    }
  }

  override def insert(typeName: String, key: String, value: T): Unit = insert(typeName, Map(key -> value))

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {
    ensureTableExists()
    val strings = kvPairs.map { case (k, v) =>
      metaDataCache.put((typeName, k), Option(v)) // note: side effect in map
      (k, serializer.serialize(typeName, v))
    }
    write(typeName, strings.toSeq)
  }

  override def invalidateCache(typeName: String, key: String): Unit = metaDataCache.invalidate((typeName, key))

  override def remove(typeName: String, key: String): Unit = {
    if (tableExists.get) {
      delete(typeName, Seq(key))
      // also remove from the cache
      metaDataCache.invalidate((typeName, key))
    } else {
      logger.debug(s"Trying to delete '$typeName:$key' but table does not exist")
    }
  }

  override def delete(typeName: String): Unit = {
    import scala.collection.JavaConversions._

    if (tableExists.get) {
      WithClose(scanValues(Some(ScanQuery(typeName)))) { keys =>
        if (keys.nonEmpty) {
          delete(typeName, keys.map(_._2).toSeq)
        }
      }
    } else {
      logger.debug(s"Trying to delete type '$typeName' but table does not exist")
    }
    metaDataCache.asMap.keys.foreach(k => if (k._1 == typeName) { metaDataCache.invalidate(k) })
  }

  // checks that the table is already created, and creates it if not
  def ensureTableExists(): Unit = tableExists.set(true, false, createTable())
}

object CachedLazyMetadata {

  val Expiry = SystemProperty("geomesa.metadata.expiry", "10 minutes")

  case class ScanQuery(typeName: String, prefix: Option[String] = None)
}
