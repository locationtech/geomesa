/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import com.typesafe.scalalogging.StrictLogging
import org.geotools.api.data.{DataStore, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.index.utils.FeatureWriterHelper
import org.locationtech.geomesa.lambda.data.LambdaDataStore.PersistenceConfig
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache._
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.locationtech.geomesa.utils.metrics.MethodProfiling

import java.io.Closeable
import java.time.{Clock, Instant, ZoneOffset, ZonedDateTime}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

/**
  * Locally cached features
  */
class KafkaFeatureCache(
    ds: DataStore,
    sft: SimpleFeatureType,
    offsetManager: OffsetManager,
    topic: String,
    persist: Option[PersistenceConfig])
   (implicit clock: Clock = Clock.systemUTC())
  extends WritableFeatureCache with ReadableFeatureCache with OffsetListener
    with Closeable with StrictLogging {

  import org.locationtech.geomesa.filter.ff

  import scala.collection.JavaConverters._

  // map of feature id -> current feature
  private val features = new ConcurrentHashMap[String, FeatureReference]

  // technically we should synchronize all access to the following array, since we expand it if needed;
  // however, in normal use it will be created up front and then only read.
  // if partitions are added at runtime, we may have synchronization issues...
  private val offsets = ArrayBuffer.empty[AtomicLong]

  private val lockTimeout = KafkaFeatureCache.LockTimeout.toMillis.get

  private val persistence = persist.map { case PersistenceConfig(e, batchSize) => new Persistence(e.toMillis, batchSize) }

  offsetManager.addOffsetListener(topic, this)

  override def partitionAssigned(partition: Int, offset: Long): Unit = {
    logger.debug(s"Partition assigned: [$topic:$partition:$offset]")
    ensurePartition(partition, offset)
  }

  override def get(id: String): SimpleFeature = {
    val result = features.get(id)
    if (result == null) { null } else { result.feature }
  }

  override def all(): Iterator[SimpleFeature] = features.values.iterator.asScala.map(_.feature)

  override def add(feature: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    if (offsets(partition).get < offset) {
      logger.trace(s"Adding [$partition:$offset] $feature created at " +
          s"${ZonedDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneOffset.UTC)}")
      features.put(feature.getID, new FeatureReference(feature, partition, offset, created))
    } else {
      logger.trace(s"Ignoring [$partition:$offset] $feature created at " +
          s"${ZonedDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneOffset.UTC)}")
    }
  }

  override def delete(feature: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    logger.trace(s"Deleting [$partition:$offset] $feature created at " +
        s"${ZonedDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneOffset.UTC)}")
    features.remove(feature.getID)
    persistence.foreach { p =>
      try {
        if (p.delete(feature.getID)) {
          logger.trace(s"Persistent store delete [$topic:$partition:$offset] $feature")
        }
      } catch {
        case NonFatal(e) => logger.error(s"Error deleting feature in persistent store: $feature", e)
      }
    }
  }

  override def offsetChanged(partition: Int, offset: Long): Unit = {
    if (offsets.length <= partition) {
      logger.debug(s"Offsets changed for [$topic:$partition]: (unassigned) -> $offset")
      ensurePartition(partition, offset)
      return
    }

    // update the valid offset
    var last = offsets(partition).get
    logger.debug(s"Offsets changed for [$topic:$partition]: $last -> $offset")
    while (last < offset && !offsets(partition).compareAndSet(last, offset)) {
      last = offsets(partition).get
    }

    def onCompleted(count: Int, time: Long): Unit =
      logger.debug(f"Size of cached state for [$topic]: ${features.size}%d (removed $count%d entries in ${time}ms)")

    profile(onCompleted _) {
      var removed = 0
      val iter = features.values().iterator()
      while (iter.hasNext) {
        val next = iter.next()
        if (next.partition == partition && next.offset <= offset) {
          iter.remove()
          removed += 1
        }
      }
      removed
    }
  }

  /**
   * Persist any expired features
   *
   * @return list of last persisted offsets per partition
   */
  def persist(): Seq[PartitionOffset] =
    persistence.getOrElse(throw new IllegalStateException("Persistence is disabled for this store")).persist()

  override def close(): Unit = {
    CloseWithLogging(persistence)
    offsetManager.removeOffsetListener(topic, this)
  }

  /**
   * Create an offset holder for the partition if it doesn't already exist
   *
   * @param partition partition
   * @param offset offset
   */
  private def ensurePartition(partition: Int, offset: Long): Unit = synchronized {
    while (offsets.length <= partition) {
      offsets += new AtomicLong(-1L)
    }
    offsets(partition).set(offset)
  }

  /**
   * Wrapper for managing scheduled persistence runs
   */
  private class Persistence(expiryMillis: Long, batchSize: Int) extends Closeable {

    private val frequency = KafkaFeatureCache.PersistInterval.toMillis.get
    private val executor = ExitingExecutor(new ScheduledThreadPoolExecutor(1))
    private val schedule = executor.scheduleWithFixedDelay(() => persist(), frequency, frequency, TimeUnit.MILLISECONDS)

    /**
     * Persist any expired features
     *
     * @return list of last persisted offsets per partition
     */
    def persist(): Seq[PartitionOffset] = {
      logger.debug(s"Running persistence for [$topic]")
      // lock per-partition to allow for multiple write threads
      // randomly access the partitions to avoid contention if multiple data stores are all on the same schedule
      Random.shuffle(offsets.indices.toList).flatMap { partition =>
        // if we don't get the lock just try again next run
        logger.trace(s"Acquiring lock for [$topic:$partition]")
        offsetManager.acquireLock(topic, partition, lockTimeout) match {
          case None =>
            logger.trace(s"Could not acquire lock for [$topic:$partition] within ${lockTimeout}ms")
            None
          case Some(lock) =>
            try {
              logger.trace(s"Acquired lock for [$topic:$partition]")
              persist(partition).map(PartitionOffset(partition, _))
            } finally {
              lock.close()
              logger.trace(s"Released lock for [$topic:$partition]")
            }
        }
      }
    }

    /**
     * Delete a feature
     *
     * @param id feature id
     * @return true if feature was deleted
     */
    def delete(id: String): Boolean = {
      WithClose(ds.getFeatureWriter(sft.getTypeName, ff.id(ff.featureId(id)), Transaction.AUTO_COMMIT)) { writer =>
        if (writer.hasNext) {
          writer.next()
          writer.remove()
          true
        } else {
          false
        }
      }
    }

    /**
     * Persist any expired features that haven't yet been persisted
     *
     * @param partition partition to persist
     * @param lastPersistedOffset result from previous recursive run, if any
     * @return offset of latest persisted feature
     */
    @tailrec
    private def persist(partition: Int, lastPersistedOffset: Option[Long] = None): Option[Long] = {
      val expiry = clock.millis() - expiryMillis

      val lastOffset = offsetManager.getOffset(topic, partition)
      logger.debug(s"Last persisted offsets for [$topic:$partition]: $lastOffset")

      var nextOffset = -1L
      // note: copy to a new collection so that any updates don't affect our persistence here
      val expired = {
        val builder = Map.newBuilder[String, FeatureReference]
        features.asScala.foreach { case (id, f) =>
          // note: offset > lastOffset ensures we don't operate on features that have already been processed
          if (f.partition == partition && f.created <= expiry && f.offset > lastOffset) {
            nextOffset = math.max(nextOffset, f.offset)
            builder += id -> f
          }
        }
        builder.result()
      }
      logger.debug(s"Found ${expired.size} expired entries for [$topic:$partition]")

      if (expired.isEmpty) {
        lastPersistedOffset
      } else {
        logger.trace(expired.values.map(e => s"offset ${e.offset}: ${e.feature}").mkString("Expired entries:\n\t", "\n\t", ""))
        val batch =
          if (expired.size <= batchSize) {
            expired
          } else {
            logger.trace(s"Skipping persistence for ${expired.size - batchSize} features based on batch size of $batchSize")
            // sort by offset since we track persistence based on offset
            val sorted = expired.toList.sortBy(_._2.offset).take(batchSize)
            nextOffset = sorted.last._2.offset
            sorted.toMap
          }
        persist(partition, batch)

        logger.debug(s"Committing offset [$topic:$partition:$nextOffset]")
        // this will trigger our listener and cause the feature to be removed from the cache
        offsetManager.setOffset(topic, partition, nextOffset)
        if (batch.size < expired.size) {
          // we skipped some expired features, so run again immediately
          persist(partition, Some(nextOffset))
        } else {
          Some(nextOffset)
        }
      }
    }

    /**
     * Persist expired features
     *
     * @param partition partition
     * @param batch expired features
     */
    private def persist(partition: Int, batch: Map[String, FeatureReference]): Unit = {
      // do an update query first
      val remaining = persistUpdates(partition, batch)
      // if any weren't updates, add them as inserts
      if (remaining.nonEmpty) {
        persistAppends(partition, remaining)
      }
    }

    /**
     * Attempt to persist expired features through modifying writes
     *
     * @param partition partition being persisted
     * @param batch expired feature
     * @return any features that were not persisted
     */
    private def persistUpdates(partition: Int, batch: Map[String, FeatureReference]): Iterable[FeatureReference] = {
      logger.debug(s"Attempting persistent updates on ${batch.size} features")
      val persistedKeys = scala.collection.mutable.Set.empty[String]
      profile((c: Int, time: Long) => logger.debug(s"Wrote $c updated feature(s) to persistent storage in ${time}ms")) {
        val filter = ff.id(batch.keys.map(ff.featureId).toSeq: _*)
        WithClose(ds.getFeatureWriter(sft.getTypeName, filter, Transaction.AUTO_COMMIT)) { writer =>
          var count = 0
          while (writer.hasNext) {
            val next = writer.next()
            batch.get(next.getID).foreach { p =>
              logger.trace(s"Persistent store modify [$topic:$partition:${p.offset}] ${p.feature}")
              persistedKeys += next.getID
              FeatureUtils.copyToFeature(next, p.feature, useProvidedFid = true)
              try {
                writer.write()
                count += 1
              } catch {
                case NonFatal(e) => logger.error(s"Error persisting feature: ${p.feature}", e)
              }
            }
          }
          count
        }
      }
      batch.collect { case (k, v) if !persistedKeys.contains(k) => v }
    }

    /**
     * Attempt to persist expired features through appending writes
     *
     * @param partition partition being persisted
     * @param batch expired feature
     */
    private def persistAppends(partition: Int, batch: Iterable[FeatureReference]): Unit = {
      logger.debug(s"Attempting persistent appends on ${batch.size} features")
      profile((c: Int, time: Long) => logger.debug(s"Wrote $c new feature(s) to persistent storage in ${time}ms")) {
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          var count = 0
          val helper = FeatureWriterHelper(writer, useProvidedFids = true)
          batch.foreach { p =>
            logger.trace(s"Persistent store append [$topic:$partition:${p.offset}] ${p.feature}")
            try {
              helper.write(p.feature)
              count += 1
            } catch {
              case NonFatal(e) => logger.error(s"Error persisting feature: ${p.feature}", e)
            }
          }
          count
        }
      }
    }

    override def close(): Unit = {
      schedule.cancel(true)
      executor.shutdownNow()
      executor.awaitTermination(1, TimeUnit.SECONDS)
    }
  }
}

object KafkaFeatureCache {

  val PersistInterval: SystemProperty = SystemProperty("geomesa.lambda.persist.interval", "1 minute")
  val LockTimeout: SystemProperty = SystemProperty("geomesa.lambda.persist.lock.timeout", "1 second")

  trait ReadableFeatureCache {

    /**
      * Returns most recent versions of all features currently in this cache
      *
      * @return
      */
    def all(): Iterator[SimpleFeature]

    /**
      * Returns the most recent version of a feature in this cache, by feature ID
      *
      * @param id feature id
      * @return
      */
    def get(id: String): SimpleFeature
  }

  trait WritableFeatureCache {

    /**
      * Initialize this cached state for a given partition and offset
      *
      * @param partition partition
      * @param offset offset
      */
    def partitionAssigned(partition: Int, offset: Long): Unit

    /**
      * Add a feature to the cached state
      *
      * @param feature feature
      * @param partition partition corresponding to the add message
      * @param offset offset corresponding to the add message
      * @param created time feature was created
      */
    def add(feature: SimpleFeature, partition: Int, offset: Long, created: Long): Unit

    /**
      * Deletes a feature from the cached state
      *
      * @param feature feature
      * @param partition partition corresponding to the delete message
      * @param offset offset corresponding to the delete message
      * @param created time feature was deleted
      */
    def delete(feature: SimpleFeature, partition: Int, offset: Long, created: Long): Unit
  }

  /**
   * Partition and offset holder
   *
   * @param partition partition
   * @param offset offset
   */
  case class PartitionOffset(partition: Int, offset: Long)

  /**
   * Feature holder used to track the latest feature in our state. Comparison is only based on the
   * partition and offset (which are unique) so that we don't have to hold onto expired features
   * in memory
   *
   * @param feature simple feature
   * @param partition kafka partition
   * @param offset kafka offset
   * @param created create time
   */
  private class FeatureReference(
      val feature: SimpleFeature,
      val partition: Int,
      val offset: Long,
      val created: Long) {
    override def equals(other: Any): Boolean = other match {
      case that: FeatureReference => partition == that.partition && offset == that.offset
      case _ => false
    }
    override def hashCode(): Int = {
      val state = Seq(partition, offset)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }
}
