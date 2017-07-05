/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.util.Comparator
import java.util.concurrent.{ConcurrentHashMap, PriorityBlockingQueue}

import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.ArrayBuffer

/**
  * Locally cached features
  */
class SharedState(partitions: Int) {

  // map of feature id -> feature
  private val features = new ConcurrentHashMap[String, SimpleFeature]

  // map of (partition, offset) -> feature
  private val offsets = new ConcurrentHashMap[(Int, Long), SimpleFeature]

  // array, indexed by partition, of queues of (offset, create time, feature), sorted by offset
  // technically we should synchronize all access to this, since we expand it if needed;
  // however, in normal use it will be created up front and then only read.
  // if partitions are added at runtime, we may have synchronization issues...
  private val expiry: ArrayBuffer[PriorityBlockingQueue[(Long, Long, SimpleFeature)]] =
    ArrayBuffer.fill(partitions)(newQueue)
  private val expiryWithIndex = expiry.zipWithIndex

  def get(id: String): SimpleFeature = features.get(id)

  def all(): Iterator[SimpleFeature] = {
    import scala.collection.JavaConverters._
    features.values.iterator.asScala
  }

  def expired: Seq[(PriorityBlockingQueue[(Long, Long, SimpleFeature)], Int)] = expiryWithIndex

  def ensurePartition(partition: Int): Unit = synchronized {
    while (expiry.length <= partition) {
      val queue = newQueue
      expiryWithIndex += ((queue, expiry.length))
      expiry += queue
    }
  }

  def add(f: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    features.put(f.getID, f)
    offsets.put((partition, offset), f)
    expiry(partition).offer((offset, created, f))
  }

  def delete(f: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = features.remove(f.getID)

  def remove(partition: Int, offset: Long): Unit = {
    val feature = offsets.remove((partition, offset))
    // only remove if there haven't been additional updates
    if (feature != null && feature.eq(features.get(feature.getID))) {
      features.remove(feature.getID)
      // note: don't remove from expiry queue, it requires a synchronous traversal
      // instead, allow persistence run to remove
    }
  }

  def debug(): String = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce
    s"features: ${features.size}, offsets: ${offsets.size}, expiry: ${expiry.map(_.size).sumOrElse(0)}"
  }

  private def newQueue = new PriorityBlockingQueue[(Long, Long, SimpleFeature)](1000, SharedState.OffsetComparator)
}

object SharedState {
  private val OffsetComparator = new Comparator[(Long, Long, SimpleFeature)]() {
    override def compare(o1: (Long, Long, SimpleFeature), o2: (Long, Long, SimpleFeature)): Int =
      java.lang.Long.compare(o1._1, o2._1)
  }
}