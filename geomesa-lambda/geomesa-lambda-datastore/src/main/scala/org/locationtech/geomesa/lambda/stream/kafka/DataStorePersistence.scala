/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.io.Closeable
import java.time.Clock
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Transaction}
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.ExpiringFeatureCache
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Random
import scala.util.control.NonFatal

/**
  * Persists expired entries to the data store
  *   1. checks for expired entries
  *   2. gets zk lock
  *   3. gets offsets from zk
  *   4. writes expired entries to data store
  *   5. updates offsets in zk
  *   6. releases zk lock
  *
  * @param ds data store to write to
  * @param sft simple feature type
  * @param offsetManager offset manager
  * @param cache shared state
  * @param topic kafka topic
  * @param ageOffMillis age off for expiring features
  * @param clock clock used for checking expiration
  */
class DataStorePersistence(ds: DataStore,
                           sft: SimpleFeatureType,
                           offsetManager: OffsetManager,
                           cache: ExpiringFeatureCache,
                           topic: String,
                           ageOffMillis: Long,
                           persistExpired: Boolean)
                          (implicit clock: Clock = Clock.systemUTC())
    extends Runnable with Closeable with MethodProfiling with LazyLogging {

  private val frequency = SystemProperty("geomesa.lambda.persist.interval").toDuration.map(_.toMillis).getOrElse(60000L)
  private val lockTimeout = SystemProperty("geomesa.lambda.persist.lock.timeout").toDuration.map(_.toMillis).getOrElse(1000L)

  private val executor = Executors.newSingleThreadScheduledExecutor()
  private val schedule = executor.scheduleWithFixedDelay(this, frequency, frequency, TimeUnit.MILLISECONDS)

  override def run(): Unit = {
    val expired = cache.expired(clock.millis() - ageOffMillis)
    logger.trace(s"Found partition(s) with expired entries in [$topic]: " +
        (if (expired.isEmpty) { "none" } else { expired.mkString(",") }))
    // lock per-partition to allow for multiple write threads
    // randomly access the partitions to avoid contention if multiple data stores are all on the same schedule
    Random.shuffle(expired).foreach { partition =>
      // if we don't get the lock just try again next run
      logger.trace(s"Acquiring lock for [$topic:$partition]")
      offsetManager.acquireLock(topic, partition, lockTimeout) match {
        case None => logger.trace(s"Could not acquire lock for [$topic:$partition] within ${lockTimeout}ms")
        case Some(lock) =>
          try {
            logger.trace(s"Acquired lock for [$topic:$partition]")
            persist(partition, clock.millis() - ageOffMillis)
          } finally {
            lock.release()
            logger.trace(s"Released lock for [$topic:$partition]")
          }
      }
    }
  }

  private def persist(partition: Int, expiry: Long): Unit = {
    import org.locationtech.geomesa.filter.ff

    val (nextOffset, expired) = cache.expired(partition, expiry)

    logger.trace(s"Found expired entries for [$topic:$partition]:\n\t" +
        expired.map { case (o, f) => s"offset $o: $f" }.mkString("\n\t"))

    val lastOffset = offsetManager.getOffset(topic, partition)
    logger.trace(s"Last persisted offsets for [$topic:$partition]: $lastOffset")

    if (expired.nonEmpty) {
      // check that features haven't been persisted yet
      val toPersist = scala.collection.mutable.Map(expired.collect { case (o, f) if o > lastOffset => f.getID -> (o, f) }: _*)

      logger.trace(s"Offsets to persist for [$topic:$partition]: ${toPersist.values.map(_._1).toSeq.sorted.mkString(",")}")

      if (!persistExpired) {
        logger.trace(s"Persist disabled for $topic")
      } else {
        def complete(modified: Long, time: Long): Unit =
          logger.debug(s"Wrote $modified updated feature(s) to persistent storage in ${time}ms")

        profile(complete _) {
          // do an update query first
          val filter = ff.id(toPersist.keys.map(ff.featureId).toSeq: _*)
          WithClose(ds.getFeatureWriter(sft.getTypeName, filter, Transaction.AUTO_COMMIT)) { writer =>
            var count = 0L
            while (writer.hasNext) {
              val next = writer.next()
              toPersist.get(next.getID).foreach { case (offset, updated) =>
                logger.trace(s"Persistent store modify [$topic:$partition:$offset] $updated")
                FeatureUtils.copyToFeature(next, updated, useProvidedFid = true)
                try { writer.write() } catch {
                  case NonFatal(e) => logger.error(s"Error persisting feature: $updated", e)
                }
                toPersist.remove(updated.getID)
              }
              count += 1
            }
            count
          }
        }

        // if any weren't updates, add them as inserts
        if (toPersist.nonEmpty) {
          def complete(appended: Long, time: Long): Unit =
            logger.debug(s"Wrote $appended new feature(s) to persistent storage in ${time}ms")

          profile(complete _) {
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
              var count = 0L
              toPersist.values.foreach { case (offset, updated) =>
                logger.trace(s"Persistent store append [$topic:$partition:$offset] $updated")
                try { FeatureUtils.write(writer, updated, useProvidedFid = true) } catch {
                  case NonFatal(e) => logger.error(s"Error persisting feature: $updated", e)
                }
                count += 1
              }
              count
            }
          }
        }
      }
    }

    if (nextOffset > lastOffset) {
      logger.trace(s"Committing offset [$topic:$partition:$nextOffset]")
      offsetManager.setOffset(topic, partition, nextOffset)
    }
  }

  override def close(): Unit = {
    schedule.cancel(true)
    executor.shutdownNow()
    executor.awaitTermination(1, TimeUnit.SECONDS)
  }
}
