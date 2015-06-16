/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Envelope
import org.locationtech.geomesa.utils.geotools.GridSnap
import scala.collection.mutable

/**
 * Spatial index that breaks up space into discrete buckets to index points.
 * Does not support non-point inserts.
 */
class BucketIndex[T](xBuckets: Int = 360,
                     yBuckets: Int = 180,
                     extents: Envelope = new Envelope(-180.0, 180.0, -90.0, 90.0)) extends SpatialIndex[T] {

  // create the buckets upfront to avoid having to synchronize the whole array
  private val gridSnap = new GridSnap(extents, xBuckets, yBuckets)
  private val buckets = Array.fill(xBuckets, yBuckets)(mutable.HashSet.empty[T])

  override def insert(envelope: Envelope, item: T): Unit = {
    val (i, j) = getBucket(envelope)
    val bucket = buckets(i)(j)
    bucket.synchronized(bucket.add(item))
  }

  override def remove(envelope: Envelope, item: T): Boolean = {
    val (i, j) = getBucket(envelope)
    val bucket = buckets(i)(j)
    bucket.synchronized(bucket.remove(item))
  }

  override def query(envelope: Envelope): Iterator[T] = {
    val mini = gridSnap.i(envelope.getMinX)
    val maxi = gridSnap.i(envelope.getMaxX)
    val minj = gridSnap.j(envelope.getMinY)
    val maxj = gridSnap.j(envelope.getMaxY)

    new Iterator[T]() {
      var i = mini
      var j = minj
      var iter = Iterator.empty.asInstanceOf[Iterator[T]]

      override def hasNext = {
        while (!iter.hasNext && i <= maxi && j <= maxj) {
          val bucket = buckets(i)(j)
          if (j < maxj) {
            j += 1
          } else {
            j = minj
            i += 1
          }
          iter = bucket.synchronized(bucket.toSeq).iterator
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
