/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import java.io.Closeable
import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Callable, Executors, Future, TimeUnit}

import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.util.ZookeeperLocking
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Update stats for a data store in a background thread. Uses distributed locking to ensure
  * that work isn't being duplicated.
  *
  * Note: currently we do not schedule stats to automatically run. Instead, we can use the
  * command line tools and OS scheduling (e.g. cron).
  *
  * @param ds data store to collect stats for
  */
class StatsRunner(ds: AccumuloDataStore) extends Runnable with Closeable {

  private val es = Executors.newSingleThreadScheduledExecutor()
  private val scheduled = new AtomicBoolean(false)
  private val shutdown  = new AtomicBoolean(false)

  /**
    * Runs updates asynchronously. Will continue scheduling itself until 'close' is called.
    *
    * @param initialDelay initial delay, in minutes
    */
  def scheduleRepeating(initialDelay: Int = 0): Unit = {
    scheduled.set(true)
    if (initialDelay > 0) {
      es.schedule(this, initialDelay, TimeUnit.MINUTES)
    } else {
      es.submit(this)
    }
  }

  /**
    * Submits a stat run for the given sft
    *
    * @param sft simple feature type
    * @param delay delay, in minutes before executing
    * @return
    */
  def submit(sft: SimpleFeatureType, delay: Int = 0): Future[Instant] = {
    val runner = new StatRunner(ds, sft)
    if (delay > 0) {
      es.schedule(runner, delay, TimeUnit.MINUTES)
    } else {
      es.submit(runner)
    }
  }

  /**
    * Checks for the last time stats were run, and runs if needed.
    * Updates metadata accordingly.
    */
  override def run(): Unit = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    // convert to iterator so we check shutdown before each update
    val sfts = ds.getTypeNames.map(ds.getSchema).iterator.filter(_ => !shutdown.get())
    // try to get an exclusive lock on the sft - if not, don't wait just move along
    val lockTimeout = Some(1000L)
    // force execution of iterator
    val minUpdate = sfts.map(new StatRunner(ds, _, lockTimeout).call()).map(_.toEpochMilli).minOption
    // wait at least one minute before running again
    val minWait = 60000L
    val nextRun = minUpdate.map(_ - Instant.now(Clock.systemUTC()).toEpochMilli).filter(_ > minWait).getOrElse(minWait)

    if (scheduled.get() && !shutdown.get()) {
      es.schedule(this, nextRun, TimeUnit.MILLISECONDS)
    }
  }

  override def close(): Unit = {
    shutdown.getAndSet(true)
    es.shutdown()
  }
}

/**
  * Callable to check and update if necessary stats for a single sft
  *
  * @param ds accumulo data store
  * @param sft simple feature type to check stats for
  * @param lockTimeout timeout for how long to wait for distributed lock, in millis.
  *                    If none, will wait indefinitely
  */
class StatRunner(ds: AccumuloDataStore, sft: SimpleFeatureType, lockTimeout: Option[Long] = None)
    extends Callable[Instant] with ZookeeperLocking {

  override val connector = ds.connector

  /**
    * Runs stats for the simple feature type
    *
    * @return time of the next scheduled update
    */
  override def call(): Instant = {
    // do a quick check on the last time stats were updated
    val updateInterval = getUpdateInterval
    val unsafeUpdate = getLastUpdate.plus(updateInterval, ChronoUnit.MINUTES)

    if (unsafeUpdate.isAfter(Instant.now(Clock.systemUTC()))) {
      unsafeUpdate
    } else {
      // get the lock and re-check, in case stats have been updated by another thread
      val lockOption = lockTimeout match {
        case None => Some(acquireDistributedLock(lockKey))
        case Some(timeout) => acquireDistributedLock(lockKey, timeout)
      }
      lockOption match {
        case None => Instant.now(Clock.systemUTC()).plus(5, ChronoUnit.MINUTES) // default to check again in 5 minutes
        case Some(lock) =>
          try {
            // reload next update now that we have the lock
            val nextUpdate = getLastUpdate.plus(updateInterval, ChronoUnit.MINUTES)
            if (nextUpdate.isAfter(Instant.now(Clock.systemUTC()))) {
              nextUpdate
            } else {
              // run the update - this updates the last update time too
              ds.stats.generateStats(sft)
              Instant.now(Clock.systemUTC()).plus(updateInterval, ChronoUnit.MINUTES)
            }
          } finally {
            lock.release()
          }
      }
    }
  }

  /**
    * Reads the time of the last update
    *
    * @return last update
    */
  private def getLastUpdate: Instant = {
    ds.metadata.read(sft.getTypeName, GeoMesaMetadata.STATS_GENERATION_KEY, cache = false) match {
      case Some(dt) => Instant.from(GeoToolsDateFormat.parse(dt))
      case None     => Instant.ofEpochSecond(0)
    }
  }

  /**
    * Reads the update interval.
    *
    * @return update interval, in minutes
    */
  private def getUpdateInterval: Long =
    // note: default is 1440 minutes (one day)
    ds.metadata.read(sft.getTypeName, GeoMesaMetadata.STATS_INTERVAL_KEY).map(_.toLong).getOrElse(1440)

  private def lockKey: String = {
    val ca = StringSerialization.alphaNumericSafeString(ds.config.catalog)
    val tn = StringSerialization.alphaNumericSafeString(sft.getTypeName)
    s"/org.locationtech.geomesa/accumulo/stats/$ca/$tn"
  }
}
