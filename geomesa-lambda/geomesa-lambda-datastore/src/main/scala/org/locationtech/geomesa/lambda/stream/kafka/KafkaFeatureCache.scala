/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache._

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer

/**
  * Locally cached features
  */
class KafkaFeatureCache(topic: String) extends WritableFeatureCache with ReadableFeatureCache
    with ExpiringFeatureCache with OffsetListener with LazyLogging {

  import scala.collection.JavaConverters._

  // map of feature id -> current feature
  private val features = new ConcurrentHashMap[String, FeatureReference]

  // technically we should synchronize all access to the following arrays, since we expand them if needed;
  // however, in normal use it will be created up front and then only read.
  // if partitions are added at runtime, we may have synchronization issues...

  // array, indexed by partition, of queues of (offset, create time, feature), sorted by offset
  private val queues = ArrayBuffer.empty[(ReentrantLock, java.util.ArrayDeque[FeatureReference])]
  private val offsets = ArrayBuffer.empty[AtomicLong]

  private val debug = logger.underlying.isDebugEnabled()

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
      val id = feature.getID
      val ref = new FeatureReference(feature, id, partition, offset, created)
      features.put(id, ref)
      val (lock, queue) = queues(partition)
      lock.lock()
      // make the feature null so that we don't keep it around in memory when we don't need to
      try { queue.addLast(new FeatureReference(null, id, partition, offset, created)) } finally {
        lock.unlock()
      }
    } else {
      logger.trace(s"Ignoring [$partition:$offset] $feature created at " +
          s"${ZonedDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneOffset.UTC)}")
    }
  }

  override def delete(feature: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    logger.trace(s"Deleting [$partition:$offset] $feature created at " +
        s"${ZonedDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneOffset.UTC)}")
    features.remove(feature.getID)
  }

  override def expired(expiry: Long): Seq[Int] = {
    val result = ArrayBuffer.empty[Int]
    var i = 0
    while (i < this.queues.length) {
      val (lock, queue) = queues(i)
      lock.lock()
      val peek = try { queue.peek } finally { lock.unlock() }
      peek match {
        case null => // no-op
        case ref => if (expiry > ref.created) { result += i }
      }
      i += 1
    }
    result.toSeq
  }

  override def expired(partition: Int, expiry: Long): ExpiredFeatures = {
    val expired = ArrayBuffer.empty[FeatureReference]
    val (lock, queue) = this.queues(partition)

    var loop = true
    while (loop) {
      lock.lock()
      val poll = queue.poll()
      if (poll == null) {
        lock.unlock()
        loop = false
      } else if (poll.created > expiry) {
        // note: add back to the queue before unlocking
        try { queue.addFirst(poll) } finally {
          lock.unlock()
        }
        loop = false
      } else {
        lock.unlock()
        expired += poll
      }
    }

    logger.debug(
      s"Checking [$topic:$partition] for expired entries: found ${expired.size} expired and ${queue.size} remaining")

    val maxExpiredOffset = if (expired.isEmpty) { -1L } else { expired(expired.length - 1).offset }

    // only remove from feature cache (and persist) if there haven't been additional updates
    val latest = expired.flatMap(ref => remove(ref))

<<<<<<< HEAD
    ExpiredFeatures(maxExpiredOffset, latest.toSeq)
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
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    ExpiredFeatures(maxExpiredOffset, latest)
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
    ExpiredFeatures(maxExpiredOffset, latest)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
    ExpiredFeatures(maxExpiredOffset, latest)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
    ExpiredFeatures(maxExpiredOffset, latest)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
=======
    ExpiredFeatures(maxExpiredOffset, latest)
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
    ExpiredFeatures(maxExpiredOffset, latest)
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
  }

  override def offsetChanged(partition: Int, offset: Long): Unit = {
    logger.debug(s"Offsets changed for [$topic:$partition]: -> $offset")

    if (queues.length <= partition) {
      ensurePartition(partition, offset)
      return
    }

    val (lock, queue) = queues(partition)

    // remove the expired features from the cache
    val (featureSize, queueSize, start) = if (!debug) { (0, 0, 0L) } else {
      (features.size, queue.size, System.currentTimeMillis())
    }

    var loop = true
    while (loop) {
      lock.lock()
      val poll = queue.poll()
      if (poll == null) {
        lock.unlock()
        loop = false
      } else if (poll.offset > offset) {
        // note: add back to the queue before unlocking
        try { queue.addFirst(poll) } finally {
          lock.unlock()
        }
        loop = false
      } else {
        lock.unlock()
        // only remove from feature cache if there haven't been additional updates
        remove(poll)
      }
    }

    // update the valid offset
    var last = offsets(partition).get
    while(last < offset && !offsets(partition).compareAndSet(last, offset)) {
      last = offsets(partition).get
    }

    logger.debug(s"Size of cached state for [$topic:$partition]: features (total): " +
        s"${diff(featureSize, features.size)}, offsets: ${diff(queueSize, queue.size)} in " +
        s"${System.currentTimeMillis() - start}ms")
  }

  private def ensurePartition(partition: Int, offset: Long): Unit = synchronized {
    while (queues.length <= partition) {
      queues += new ReentrantLock -> new java.util.ArrayDeque[FeatureReference]
      offsets += new AtomicLong(-1L)
    }
    offsets(partition).set(offset)
  }

  /**
   * Conditionally removes the simple feature from the feature cache if it is the latest version. This
   * should only be called with items pulled of the expiry queue
   *
   * @param feature feature reference
   * @return
   */
  private def remove(feature: FeatureReference): Option[FeatureReference] = {
    // since the features are added to the queue after they are added to the cache,
    // this must be either the feature corresponding to the reference, or a later feature
    val current = features.get(feature.id)
    // the remove is based on the offset and partition, so if it is successful, we know that we
    // got the right feature above
    if (features.remove(feature.id, feature)) { Option(current) } else { None }
  }

  // debug message
  private def diff(original: Int, updated: Int): String = f"$updated%d (${updated - original}%+d)"
}

object KafkaFeatureCache {

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

  trait ExpiringFeatureCache {

    /**
      * Checks for any expired features
      *
      * @param expiry expiry
      * @return partitions which may contain expired features, if any
      */
    def expired(expiry: Long): Seq[Int]

    /**
      * Remove and return any expired features
      *
      * @param partition partition
      * @param expiry expiry
      * @return (maxExpiredOffset, (offset, expired feature)), ordered by offset
      */
    def expired(partition: Int, expiry: Long): ExpiredFeatures
  }

  case class ExpiredFeatures(maxOffset: Long, features: Seq[OffsetFeature])

  /**
   * Holder for a feature plus offset
   */
  trait OffsetFeature {
    def offset: Long
    def feature: SimpleFeature
  }

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
      val id: String,
      val partition: Int,
      val offset: Long,
      val created: Long) extends OffsetFeature {
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
