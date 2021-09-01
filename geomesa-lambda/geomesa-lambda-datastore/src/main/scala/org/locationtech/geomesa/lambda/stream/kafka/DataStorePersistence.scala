/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
import java.io.Closeable
import java.time.Clock
import java.util.concurrent.{Executors, TimeUnit}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{DataStore, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.{ExpiredFeatures, ExpiringFeatureCache, OffsetFeature}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling

import java.io.Closeable
import java.time.Clock
import java.util.concurrent.{Executors, TimeUnit}
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

    val ExpiredFeatures(nextOffset, expired) = cache.expired(partition, expiry)

    logger.trace(s"Found ${expired.size} expired entries for [$topic:$partition]:\n\t" +
        expired.map(e => s"offset ${e.offset}: ${e.feature}").mkString("\n\t"))

    val lastOffset = offsetManager.getOffset(topic, partition)
    logger.trace(s"Last persisted offsets for [$topic:$partition]: $lastOffset")

    if (expired.nonEmpty) {
      if (!persistExpired) {
        logger.trace(s"Persist disabled for $topic")
      } else {
        // check that features haven't been persisted yet
        val toPersist = scala.collection.mutable.Map.empty[String, OffsetFeature]
        expired.foreach { e =>
          if (e.offset > lastOffset) {
            toPersist += e.feature.getID -> e
          }
        }

        logger.trace(
          s"Offsets to persist for [$topic:$partition]: ${toPersist.values.map(_.offset).toSeq.sorted.mkString(",")}")

        def complete(modified: Long, time: Long): Unit =
          logger.debug(s"Wrote $modified updated feature(s) to persistent storage in ${time}ms")

        profile(complete _) {
          // do an update query first
          val filter = ff.id(toPersist.keys.map(ff.featureId).toSeq: _*)
          WithClose(ds.getFeatureWriter(sft.getTypeName, filter, Transaction.AUTO_COMMIT)) { writer =>
            var count = 0L
            while (writer.hasNext) {
              val next = writer.next()
              toPersist.get(next.getID).foreach { p =>
                logger.trace(s"Persistent store modify [$topic:$partition:${p.offset}] ${p.feature}")
                FeatureUtils.copyToFeature(next, p.feature, useProvidedFid = true)
                try { writer.write() } catch {
                  case NonFatal(e) => logger.error(s"Error persisting feature: ${p.feature}", e)
                }
                toPersist.remove(p.feature.getID)
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
              toPersist.values.foreach { p =>
                logger.trace(s"Persistent store append [$topic:$partition:${p.offset}] ${p.feature}")
                try { FeatureUtils.write(writer, p.feature, useProvidedFid = true) } catch {
                  case NonFatal(e) => logger.error(s"Error persisting feature: ${p.feature}", e)
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
