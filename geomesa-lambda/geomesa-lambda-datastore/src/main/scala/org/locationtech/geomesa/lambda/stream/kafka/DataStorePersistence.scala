/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.io.Closeable
import java.time.Clock
import java.util.concurrent.{PriorityBlockingQueue, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Transaction}
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
  * @param state shared state
  * @param topic kafka topic
  * @param ageOffMillis age off for expiring features
  * @param clock clock used for checking expiration
  */
class DataStorePersistence(ds: DataStore,
                           sft: SimpleFeatureType,
                           offsetManager: OffsetManager,
                           state: SharedState,
                           topic: String,
                           ageOffMillis: Long,
                           persistExpired: Boolean)
                          (implicit clock: Clock = Clock.systemUTC())
    extends Runnable with Closeable with LazyLogging {

  private val frequency = SystemProperty("geomesa.lambda.persist.interval").toDuration.getOrElse(60000L)
  private val schedule = KafkaStore.executor.scheduleWithFixedDelay(this, 0L, frequency, TimeUnit.MILLISECONDS)
  private val lockTimeout = SystemProperty("geomesa.lambda.persist.lock.timeout").toDuration.getOrElse(1000L)

  override def run(): Unit = {
    val expired = state.expired.filter(e => checkPartition(e._1))
    logger.trace(s"Found ${expired.length} partition(s) with expired entries in [$topic]")
    // lock per-partition to allow for multiple write threads
    expired.foreach { case (queue, partition) =>
      // if we don't get the lock just try again next run
      logger.trace(s"Acquiring lock for [$topic:$partition]")
      offsetManager.acquireLock(topic, partition, lockTimeout) match {
        case None => logger.trace(s"Could not acquire lock for [$topic:$partition] within ${lockTimeout}ms")
        case Some(lock) =>
          try {
            logger.trace(s"Acquired lock for [$topic:$partition]")
            persist(partition, queue, clock.millis() - ageOffMillis)
          } finally {
            lock.release()
            logger.trace(s"Released lock for [$topic:$partition]")
          }
      }
    }
  }

  private def checkPartition(expiry: PriorityBlockingQueue[(Long, Long, SimpleFeature)]): Boolean = {
    expiry.peek() match {
      case null => false
      case (_, created, _) => (clock.millis() - ageOffMillis) > created
    }
  }

  private def persist(partition: Int, queue: PriorityBlockingQueue[(Long, Long, SimpleFeature)], expiry: Long): Unit = {
    import org.locationtech.geomesa.filter.ff

    val expired = scala.collection.mutable.Queue.empty[(Long, Long, SimpleFeature)]

    var loop = true
    while (loop) {
      queue.poll() match {
        case null => loop = false
        case f if f._2 > expiry => queue.offer(f); loop = false
        case f => expired += f
      }
    }

    logger.trace(s"Found expired entries for [$topic:$partition]:\n\t" +
        expired.map { case (o, created, f) => s"offset $o: $f created at ${new DateTime(created, DateTimeZone.UTC)}" }.mkString("\n\t"))

    if (expired.nonEmpty) {
      var lastOffset = offsetManager.getOffset(topic, partition)
      logger.trace(s"Last persisted offsets for [$topic:$partition]: $lastOffset")
      // check that features haven't been persisted yet, haven't been deleted, and have no additional updates pending
      val toPersist = {
        val latest = expired.filter { case (offset, _, f) =>
          if (lastOffset <= offset) {
            // update the offsets we will write
            lastOffset = offset + 1L
            // ensure not deleted and no additional updates pending
            f.eq(state.get(f.getID))
          } else {
            false
          }
        }
        scala.collection.mutable.Map(latest.map(f => (f._3.getID,  f)): _*)
      }

      logger.trace(s"Offsets to persist for [$topic:$partition]: ${toPersist.values.map(_._1).toSeq.sorted.mkString(",")}")

      if (toPersist.nonEmpty) {
        if (!persistExpired) {
          logger.trace(s"Persist disabled for $topic")
        } else {
          // do an update query first
          val filter = ff.id(toPersist.keys.map(ff.featureId).toSeq: _*)
          WithClose(ds.getFeatureWriter(sft.getTypeName, filter, Transaction.AUTO_COMMIT)) { writer =>
            while (writer.hasNext) {
              val next = writer.next()
              toPersist.get(next.getID).foreach { case (offset, created, updated) =>
                logger.trace(s"Persistent store modify [$topic:$partition:$offset] $updated created at " +
                    s"${new DateTime(created, DateTimeZone.UTC)}")
                FeatureUtils.copyToFeature(next, updated, useProvidedFid = true)
                try { writer.write() } catch {
                  case NonFatal(e) => logger.error(s"Error persisting feature: $updated", e)
                }
                toPersist.remove(updated.getID)
              }
            }
          }
          // if any weren't updates, add them as inserts
          if (toPersist.nonEmpty) {
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
              toPersist.values.foreach { case (offset, created, updated) =>
                logger.trace(s"Persistent store append [$topic:$partition:$offset] $updated created at " +
                    s"${new DateTime(created, DateTimeZone.UTC)}")
                FeatureUtils.copyToWriter(writer, updated, useProvidedFid = true)
                try { writer.write() } catch {
                  case NonFatal(e) => logger.error(s"Error persisting feature: $updated", e)
                }
              }
            }
          }
        }
        logger.trace(s"Committing offset [$topic:$partition:$lastOffset]")
        offsetManager.setOffset(topic, partition, lastOffset)
      }
    }
  }

  override def close(): Unit = schedule.cancel(true)
}
