/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.metadata

import com.github.benmanes.caffeine.cache.{Cache, CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.locationtech.geomesa.utils.text.DateParsing

import java.io.Closeable
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.time.{Instant, ZoneOffset}
import java.util.Locale
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import scala.util.Try

/**
  * Metadata persisted in a database table. The underlying table will be lazily created when required.
  * Metadata values are cached with a configurable timeout to save repeated database reads.
  *
  * @tparam T type param
  */
trait TableBasedMetadata[T] extends GeoMesaMetadata[T] with LazyLogging {

  import scala.collection.JavaConverters._

  /**
    * Serializer
    *
    * @return
    */
  def serializer: MetadataSerializer[T]

  /**
    * Checks if the underlying table exists
    *
    * @return
    */
  protected def checkIfTableExists: Boolean

  /**
    * Creates the underlying table. Must be idempotent.
    */
  protected def createTable(): Unit

  /**
    * Create an instance to use for backup
    *
    * @param timestamp formatted timestamp for the current time
    * @return
    */
  protected def createEmptyBackup(timestamp: String): TableBasedMetadata[T]

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
  protected def delete(typeName: String, keys: Seq[String]): Unit

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

  private val expiry = TableBasedMetadata.Expiry.toDuration.get.toMillis

  private val tableExists = new TableExists().init()

  // cache for our metadata - invalidate every 10 minutes so we keep things current
  private val metaDataCache: LoadingCache[(String, String), Option[T]] =
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
  private val metaDataScanCache: LoadingCache[(String, String), Seq[(String, T)]] =
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

  private lazy val formatter =
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendValue(ChronoField.YEAR, 4)
      .appendValue(ChronoField.MONTH_OF_YEAR, 2)
      .appendValue(ChronoField.DAY_OF_MONTH, 2)
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .toFormatter(Locale.US)
      .withZone(ZoneOffset.UTC)

  override def getFeatureTypes: Array[String] = {
    if (!tableExists.get) { Array.empty } else {
      WithClose(scanKeys()) { keys =>
        keys.collect { case (typeName, key) if key == GeoMesaMetadata.AttributesKey => typeName }.toArray
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
    if (tableExists.get()) {
      delete(typeName, keys)
      // also remove from the cache
      keys.foreach(k => metaDataCache.invalidate((typeName, k)))
      invalidate(metaDataScanCache, typeName)
    } else {
      logger.debug(s"Trying to delete '$typeName: ${keys.mkString(", ")}' but table does not exist")
    }
  }

  override def delete(typeName: String): Unit = {
    if (tableExists.get()) {
      WithClose(scanValues(typeName)) { rows =>
        if (rows.nonEmpty) {
          delete(typeName, rows.map(_._1).toSeq)
        }
      }
    } else {
      logger.debug(s"Trying to delete type '$typeName' but table does not exist")
    }
    Seq(metaDataCache, metaDataScanCache).foreach(invalidate(_, typeName))
  }

  override def backup(typeName: String): Unit = {
    if (tableExists.get()) {
      WithClose(scanValues(typeName)) { rows =>
        if (rows.nonEmpty) {
          WithClose(createEmptyBackup(DateParsing.formatInstant(Instant.now, formatter))) { metadata =>
            metadata.ensureTableExists()
            metadata.write(typeName, rows.toSeq)
          }
        }
      }
    } else {
      logger.debug(s"Trying to back up type '$typeName' but table does not exist")
    }
  }

  /**
   * Checks that the table is already created, and creates it if not
   */
  def ensureTableExists(): Unit = tableExists.createIfNeeded()

  override def resetCache(): Unit = {
    tableExists.run()
    metaDataCache.invalidateAll()
    metaDataScanCache.invalidateAll()
  }

  override def close(): Unit = tableExists.close()

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

  /**
   * Tracks existence of the metadata table
   */
  private class TableExists extends Runnable with Closeable {

    @volatile
    private var exists = false
    private var es: ScheduledExecutorService = _
    private var future: ScheduledFuture[_] = _

    /**
     * Initialization
     *
     * @return
     */
    def init(): TableExists = { run(); this }

    /**
     * Does the table exist
     *
     * @return
     */
    def get(): Boolean = exists

    /**
     * Create the table if it doesn't exist.
     *
     * Note that we don't synchronize this method, but creating the table should be idempotent
     */
    def createIfNeeded(): Unit = {
      if (!exists) {
        createTable()
        run()
      }
    }

    /**
     * Check for table existence
     */
    override def run(): Unit = {
      exists = checkIfTableExists
      if (exists) {
        cancel()
      } else {
        schedule()
      }
    }

    override def close(): Unit = cancel()

    private def schedule(): Unit = synchronized {
      if (es == null) {
        es = ExitingExecutor(new ScheduledThreadPoolExecutor(1))
      }
      if (future == null) {
        future = es.scheduleAtFixedRate(this, expiry, expiry, TimeUnit.MILLISECONDS)
      }
    }

    /**
     * Cancel any scheduled tasks
     */
    private def cancel(): Unit = synchronized {
      if (future != null) {
        Try(future.cancel(true))
        future = null
      }
      if (es != null) {
        CloseWithLogging(es)
        es = null
      }
    }
  }
}

object TableBasedMetadata {
  val Expiry: SystemProperty = SystemProperty("geomesa.metadata.expiry", "10 minutes")
}
