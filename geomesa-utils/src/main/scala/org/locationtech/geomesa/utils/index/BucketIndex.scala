/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import com.vividsolutions.jts.geom.Envelope
import org.locationtech.geomesa.utils.geotools.GridSnap

/**
 * Spatial index that breaks up space into discrete buckets to index points.
 * Does not support non-point inserts.
 */
class BucketIndex[T](xBuckets: Int = 360,
                     yBuckets: Int = 180,
                     extents: Envelope = new Envelope(-180.0, 180.0, -90.0, 90.0)) extends SpatialIndex[T] {

  private val gridSnap = new GridSnap(extents, xBuckets, yBuckets)
  // create the buckets up front to avoid having to synchronize the whole array
  // we use a concurrentHashSet, which gives us iterators that aren't affected by modifications to the backing set
  private def newSet = Collections.newSetFromMap(new ConcurrentHashMap[T, java.lang.Boolean])
  private val buckets = Array.fill(xBuckets, yBuckets)(newSet)

  override def insert(envelope: Envelope, item: T): Unit = {
    val (i, j) = getBucket(envelope)
    buckets(i)(j).add(item)
  }

  override def remove(envelope: Envelope, item: T): Boolean = {
    val (i, j) = getBucket(envelope)
    buckets(i)(j).remove(item)
  }

  override def query(envelope: Envelope): Iterator[T] = {
    val mini = snapX(envelope.getMinX)
    val maxi = snapX(envelope.getMaxX)
    val minj = snapY(envelope.getMinY)
    val maxj = snapY(envelope.getMaxY)

    new Iterator[T]() {
      import scala.collection.JavaConverters._

      private var i = mini
      private var j = minj
      private var iter = Iterator.empty.asInstanceOf[Iterator[T]].asJava

      override def hasNext: Boolean = {
        while (!iter.hasNext && i <= maxi && j <= maxj) {
          val bucket = buckets(i)(j)
          if (j < maxj) {
            j += 1
          } else {
            j = minj
            i += 1
          }
          iter = bucket.iterator()
        }
        iter.hasNext
      }

      override def next(): T = iter.next()
    }
  }

  private def getBucket(envelope: Envelope): (Int, Int) = {
    val (x, y) = SpatialIndex.getCenter(envelope)
    (snapX(x), snapY(y))
  }

  private def snapX(x: Double): Int = {
    val i = gridSnap.i(x)
    if (i != -1) { i } else if (x < extents.getMinX) { 0 } else { xBuckets - 1 }
  }

  private def snapY(y: Double): Int = {
    val j = gridSnap.j(y)
    if (j != -1) { j } else if (y < extents.getMinY) { 0 } else { yBuckets - 1 }
  }
}
