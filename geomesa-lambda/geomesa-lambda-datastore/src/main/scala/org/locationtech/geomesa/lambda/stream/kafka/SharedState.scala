/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.util.Comparator
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, PriorityBlockingQueue}

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.ArrayBuffer

/**
  * Locally cached features
  */
class SharedState(topic: String, partitions: Int, expire: Boolean) extends OffsetListener with LazyLogging {

  // map of feature id -> current feature
  private val features = new ConcurrentHashMap[String, SimpleFeature]

  // technically we should synchronize all access to the following arrays, since we expand them if needed;
  // however, in normal use it will be created up front and then only read.
  // if partitions are added at runtime, we may have synchronization issues...

  // array, indexed by partition, of maps of offset -> feature, for all features we have added
  private val offsets = ArrayBuffer.fill(partitions)(new ConcurrentHashMap[Long, SimpleFeature])
  // last offset that hasn't been expired
  private val validOffsets = ArrayBuffer.fill(partitions)(new AtomicLong(-1L))

  // array, indexed by partition, of queues of (offset, create time, feature), sorted by offset
  private val expiry = ArrayBuffer.fill(partitions)(newQueue)

  private val debug = logger.underlying.isDebugEnabled()

  /**
    * Returns the most recent version of a feature in this cache, by feature ID
    *
    * @param id feature id
    * @return
    */
  def get(id: String): SimpleFeature = features.get(id)

  /**
    * Returns most recent versions of all features currently in this cache
    *
    * @return
    */
  def all(): Iterator[SimpleFeature] = {
    import scala.collection.JavaConverters._
    features.values.iterator.asScala
  }

  /**
    * Add a feature to the cached state
    *
    * @param f feature
    * @param partition partition corresponding to the add message
    * @param offset offset corresponding to the add message
    * @param created time feature was created
    */
  def add(f: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    val valid = validOffsets(partition).get
    if (valid <= offset) {
      logger.trace(s"Adding [$partition:$offset] $f created at ${new DateTime(created, DateTimeZone.UTC)}")
      features.put(f.getID, f)
      offsets(partition).put(offset, f)
      if (expire) {
        expiry(partition).offer((offset, created, f))
      }
    } else {
      logger.trace(s"Ignoring [$partition:$offset] $f created at ${new DateTime(created, DateTimeZone.UTC)}, valid offset is $valid")
    }
  }

  /**
    * Removes a feature from the cached state, for instance upon delete
    *
    * @param f feature
    * @param partition partition corresponding to the delete message
    * @param offset offset corresponding to the delete message
    * @param created time feature was deleted
    */
  def delete(f: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    logger.trace(s"Deleting [$partition:$offset] $f created at ${new DateTime(created, DateTimeZone.UTC)}")
    features.remove(f.getID)
  }

  /**
    * Initialize this cached state for a given partition and offset
    *
    * @param partition partition
    * @param offset offset
    */
  def partitionAssigned(partition: Int, offset: Long): Unit = synchronized {
    while (expiry.length <= partition) {
      expiry += newQueue
      offsets += new ConcurrentHashMap[Long, SimpleFeature]
      validOffsets += new AtomicLong(-1L)
    }
    if (validOffsets(partition).get < offset) {
      validOffsets(partition).set(offset)
    }
  }

  /**
    * Thread safe check for any expired features
    *
    * @param expiry expiry
    * @return
    */
  def expired(expiry: Long): Seq[Int] = {
    val result = ArrayBuffer.empty[Int]
    var i = 0
    while (i < this.expiry.length) {
      this.expiry(i).peek match {
        case null => // no-op
        case (_, created, _) => if (expiry > created) { result += i }
      }
      i += 1
    }
    result
  }

  /**
    * Retrieves and removes any expired entries.
    *
    * Thread safe for different partitions, but not for a single partition
    *
    * @param partition partition
    * @param expiry expiry
    * @return expired features, ordered by offset
    */
  def expired(partition: Int, expiry: Long): Seq[(Long, SimpleFeature)] = {
    val expired = scala.collection.mutable.Queue.empty[(Long, SimpleFeature)]
    val queue = this.expiry(partition)
    var loop = true
    while (loop) {
      queue.poll() match {
        case null => loop = false
        case f if f._2 > expiry => queue.offer(f); loop = false
        case (offset, _, feature) => expired += ((offset, feature))
      }
    }
    logger.debug(s"Checking [$topic:$partition] for expired entries: found ${expired.size} expired and ${queue.size} remaining")
    expired
  }

  override def offsetChanged(partition: Int, offset: Long): Unit = {
    var current = validOffsets(partition).get
    // ensure that we are operating on a valid range of offsets
    while (!validOffsets(partition).compareAndSet(current, offset)) {
      current = validOffsets(partition).get
    }
    val partitionOffsets = offsets(partition)

    // remove the expired features from the cache
    if (current == -1L) {
      logger.debug(s"Setting initial offset [$topic:$partition:$offset]")
    } else {
      val (featureSize, offsetSize, start) = if (!debug) { (0, 0, 0L) } else {
        logger.debug(s"Offsets changed for [$topic:$partition]: $current->$offset")
        (features.size, partitionOffsets.size, System.currentTimeMillis())
      }
      while (current < offset) {
        val feature = partitionOffsets.remove(current)
        if (feature == null) {
          logger.trace(s"Tried to remove [$partition:$current], but it was not present in the offsets map")
        } else if (feature.eq(features.get(feature.getID))) {
          // ^ only remove from feature cache if there haven't been additional updates
          features.remove(feature.getID)
          // note: don't remove from expiry queue, it requires a synchronous traversal
          // instead, allow persistence run to remove
        }
        current += 1L
      }
      logger.debug(s"Size of cached state for [$topic:$partition]: features (total): " +
          s"${diff(featureSize, features.size)}, offsets: ${diff(offsetSize, partitionOffsets.size)} in " +
          s"${System.currentTimeMillis() - start}ms")
    }
  }

  // debug message
  private def diff(original: Int, updated: Int): String = f"$updated%d (${updated - original}%+d)"

  private def newQueue = new PriorityBlockingQueue[(Long, Long, SimpleFeature)](1000, SharedState.OffsetComparator)
}

object SharedState {
  private val OffsetComparator = new Comparator[(Long, Long, SimpleFeature)]() {
    override def compare(o1: (Long, Long, SimpleFeature), o2: (Long, Long, SimpleFeature)): Int =
      java.lang.Long.compare(o1._1, o2._1)
  }
}