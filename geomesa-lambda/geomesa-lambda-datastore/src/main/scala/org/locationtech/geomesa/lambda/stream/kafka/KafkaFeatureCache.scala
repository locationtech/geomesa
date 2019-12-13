/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.{ExpiringFeatureCache, ReadableFeatureCache, WritableFeatureCache}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.ArrayBuffer

/**
  * Locally cached features
  */
class KafkaFeatureCache(topic: String) extends WritableFeatureCache with ReadableFeatureCache
    with ExpiringFeatureCache with OffsetListener with LazyLogging {

  // map of feature id -> current feature
  private val features = new ConcurrentHashMap[String, SimpleFeature]

  // technically we should synchronize all access to the following arrays, since we expand them if needed;
  // however, in normal use it will be created up front and then only read.
  // if partitions are added at runtime, we may have synchronization issues...

  // array, indexed by partition, of queues of (offset, create time, feature), sorted by offset
  private val queues = ArrayBuffer.empty[(ReentrantLock, java.util.ArrayDeque[(Long, Long, SimpleFeature)])]
  private val offsets = ArrayBuffer.empty[AtomicLong]

  private val debug = logger.underlying.isDebugEnabled()

  override def partitionAssigned(partition: Int, offset: Long): Unit = {
    logger.debug(s"Partition assigned: [$topic:$partition:$offset]")
    ensurePartition(partition, offset)
  }

  override def get(id: String): SimpleFeature = features.get(id)

  override def all(): Iterator[SimpleFeature] = {
    import scala.collection.JavaConverters._
    features.values.iterator.asScala
  }

  override def add(feature: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    if (offsets(partition).get < offset) {
      logger.trace(s"Adding [$partition:$offset] $feature created at " +
          s"${ZonedDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneOffset.UTC)}")
      features.put(feature.getID, feature)
      val (lock, queue) = queues(partition)
      lock.lock()
      try { queue.addLast((offset, created, feature)) } finally {
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
        case (_, created, _) => if (expiry > created) { result += i }
      }
      i += 1
    }
    result
  }

  override def expired(partition: Int, expiry: Long): (Long, Seq[(Long, SimpleFeature)]) = {
    val expired = ArrayBuffer.empty[(Long, SimpleFeature)]
    val (lock, queue) = this.queues(partition)

    var loop = true
    while (loop) {
      lock.lock()
      val poll = queue.poll()
      if (poll == null) {
        lock.unlock()
        loop = false
      } else if (poll._2 > expiry) {
        // note: add back to the queue before unlocking
        try { queue.addFirst(poll) } finally {
          lock.unlock()
        }
        loop = false
      } else {
        lock.unlock()
        expired += ((poll._1, poll._3))
      }
    }

    logger.debug(s"Checking [$topic:$partition] for expired entries: found ${expired.size} expired and ${queue.size} remaining")

    val maxExpiredOffset = if (expired.isEmpty) { -1L } else { expired(expired.length - 1)._1 }

    // only remove from feature cache (and persist) if there haven't been additional updates
    val latest = expired.filter { case (_, feature) => remove(feature) }

    (maxExpiredOffset, latest)
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
      } else if (poll._1 > offset) {
        // note: add back to the queue before unlocking
        try { queue.addFirst(poll) } finally {
          lock.unlock()
        }
        loop = false
      } else {
        lock.unlock()
        // only remove from feature cache if there haven't been additional updates
        remove(poll._3)
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
      queues += ((new ReentrantLock, new java.util.ArrayDeque[(Long, Long, SimpleFeature)]))
      offsets += new AtomicLong(-1L)
    }
    offsets(partition).set(offset)
  }

  // conditionally removes the simple feature from the feature cache if it is the latest version
  private def remove(feature: SimpleFeature): Boolean = {
    // note: there isn't an atomic remove that checks identity, so check first and then do an equality remove.
    // there is a small chance that the feature will be updated in between the identity and equality checks,
    // and removed incorrectly, however the alternative is full synchronization on inserts and deletes.
    // also, with standard usage patterns of many updates and only a few writes, the first check (which is
    // cheaper) will be false, and we can short-circuit the second check
    feature.eq(features.get(feature.getID)) && features.remove(feature.getID, feature)
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
    def expired(partition: Int, expiry: Long): (Long, Seq[(Long, SimpleFeature)])
  }
}
