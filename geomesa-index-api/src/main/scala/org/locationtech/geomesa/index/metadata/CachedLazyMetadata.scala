/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.collection.{CloseableIterator, IsSynchronized, MaybeSynchronized, NotSynchronized}

/**
  * Backs metadata with a cache to save repeated database reads. Underlying table will be lazily created
  * when required.
  *
  * @tparam T type param
  */
trait CachedLazyMetadata[T] extends GeoMesaMetadata[T] with LazyLogging {

  this: MetadataAdapter =>

  // separator used between type names and keys
  val typeNameSeparator: Char = '~'

  protected def serializer: MetadataSerializer[T]

  // only synchronize if table doesn't exist - otherwise it's ready only and we can avoid synchronization
  private val tableExists: MaybeSynchronized[Boolean] =
    if (checkIfTableExists) { new NotSynchronized(true) } else { new IsSynchronized(false) }

  // cache for our metadata - invalidate every 10 minutes so we keep things current
  private val metaDataCache =
    Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(
      new CacheLoader[(String, String), Option[T]] {
        override def load(k: (String, String)): Option[T] = {
          if (tableExists.get) {
            val (typeName, key) = k
            scanValue(encodeRow(typeName, key)).flatMap(b => Option(serializer.deserialize(typeName, key, b)))
          } else {
            None
          }
        }
      }
    )

  override def getFeatureTypes: Array[String] = {
    if (tableExists.get) {
      val rows = scanRows(None)
      try {
        rows.map(r => CachedLazyMetadata.decodeRow(r, typeNameSeparator)._1).toSeq.distinct.toArray
      } finally {
        rows.close()
      }
    } else {
      Array.empty
    }
  }

  override def read(typeName: String, key: String, cache: Boolean): Option[T] = {
    if (!cache) {
      metaDataCache.invalidate((typeName, key))
    }
    metaDataCache.get((typeName, key))
  }

  override def insert(typeName: String, key: String, value: T): Unit = insert(typeName, Map(key -> value))

  override def insert(typeName: String, kvPairs: Map[String, T]): Unit = {
    ensureTableExists()
    val inserts = kvPairs.toSeq.map { case (k, v) =>
      // note: side effect in .map - pre-fetch into the cache
      metaDataCache.put((typeName, k), Option(v))
      (encodeRow(typeName, k), serializer.serialize(typeName, k, v))
    }
    write(inserts)
  }

  override def invalidateCache(typeName: String, key: String): Unit = metaDataCache.invalidate((typeName, key))

  override def remove(typeName: String, key: String): Unit = {
    if (tableExists.get) {
      delete(encodeRow(typeName, key))
      // also remove from the cache
      metaDataCache.invalidate((typeName, key))
    } else {
      logger.debug(s"Trying to delete '$typeName:$key' but table does not exist")
    }
  }

  override def delete(typeName: String): Unit = {
    import scala.collection.JavaConversions._

    if (tableExists.get) {
      val prefix = encodeRow(typeName, "")
      val rows = scanRows(Some(prefix))
      try {
        val all = rows.toBuffer
        if (all.nonEmpty) {
          delete(all)
        }
      } finally {
        rows.close()
      }
    } else {
      logger.debug(s"Trying to delete type '$typeName' but table does not exist")
    }
    metaDataCache.asMap.keys.foreach(k => if (k._1 == typeName) { metaDataCache.invalidate(k) })
  }

  def encodeRow(typeName: String, key: String): Array[Byte] =
    CachedLazyMetadata.encodeRow(typeName, key, typeNameSeparator)

  // checks that the table is already created, and creates it if not
  private def ensureTableExists(): Unit = tableExists.set(true, false, createTable())
}

object CachedLazyMetadata {

  def encodeRow(typeName: String, key: String, separator: Char): Array[Byte] = {
    // escaped to %U+XXXX unicode since decodeRow splits by separator
    val ESCAPE = s"%${"U+%04X".format(separator.toInt)}"
    s"${typeName.replace(separator.toString, ESCAPE)}$separator$key".getBytes(StandardCharsets.UTF_8)
  }

  def decodeRow(row: Array[Byte], separator: Char): (String, String) = {
    // escaped to %U+XXXX unicode since decodeRow splits by separator
    val ESCAPE = s"%${"U+%04X".format(separator.toInt)}"
    val all = new String(row, StandardCharsets.UTF_8)
    val split = all.indexOf(separator)
    (all.substring(0, split).replace(ESCAPE, separator.toString), all.substring(split + 1, all.length))
  }
}

/**
  * Binding for underlying database. Methods should not be synchronized
  */
trait MetadataAdapter extends Closeable {

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
    * Writes row/value pairs
    *
    * @param rows row/values
    */
  protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit

  /**
    * Deletes a row
    *
    * @param row row
    */
  protected def delete(row: Array[Byte])

  /**
    * Deletes multiple rows
    *
    * @param rows rows
    */
  protected def delete(rows: Seq[Array[Byte]])

  /**
    * Reads a value from the underlying table
    *
    * @param row row
    * @return value, if it exists
    */
  protected def scanValue(row: Array[Byte]): Option[Array[Byte]]

  /**
    * Reads row keys from the underlying table
    *
    * @param prefix row key prefix
    * @return matching row keys (not values)
    */
  protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[Array[Byte]]
}