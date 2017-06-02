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
    val bucket = buckets(i)(j)
    bucket.add(item)
  }

  override def remove(envelope: Envelope, item: T): Boolean = {
    val (i, j) = getBucket(envelope)
    val bucket = buckets(i)(j)
    bucket.remove(item)
  }

  override def query(envelope: Envelope): Iterator[T] = {
    val mini = gridSnap.i(envelope.getMinX)
    val maxi = gridSnap.i(envelope.getMaxX)
    val minj = gridSnap.j(envelope.getMinY)
    val maxj = gridSnap.j(envelope.getMaxY)

    new Iterator[T]() {
      import scala.collection.JavaConverters._

      var i = mini
      var j = minj
      var iter = Iterator.empty.asInstanceOf[Iterator[T]].asJava

      override def hasNext = {
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

      override def next() = iter.next()
    }
  }

  private def getBucket(envelope: Envelope): (Int, Int) = {
    val (x, y) = SpatialIndex.getCenter(envelope)
    (gridSnap.i(x), gridSnap.j(y))
  }
}
