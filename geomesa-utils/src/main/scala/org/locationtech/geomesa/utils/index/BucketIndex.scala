/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Envelope, Geometry, Point}
import org.locationtech.geomesa.utils.geotools.GridSnap

import scala.annotation.tailrec

/**
  * Spatial index that breaks up space into discrete buckets to index points
  *
  * Does not support non-point inserts
  *
  * @param xBuckets number of x buckets
  * @param yBuckets number of y buckets
  * @param extents area to be indexed
  * @tparam T index value binding
  */
class BucketIndex[T](xBuckets: Int = 360,
                     yBuckets: Int = 180,
                     extents: Envelope = new Envelope(-180.0, 180.0, -90.0, 90.0))
    extends SpatialIndex[T] with LazyLogging {

  // create the buckets up front to avoid having to synchronize the whole array
  // we use a ConcurrentHashMap, which gives us iterators that aren't affected by modifications to the backing map
  private val buckets = Array.fill(xBuckets, yBuckets)(new ConcurrentHashMap[String, T]())

  private val gridSnap = new GridSnap(extents, xBuckets, yBuckets)

  override def insert(geom: Geometry, key: String, value: T): Unit = {
    val pt = geom.asInstanceOf[Point]
    val i = snapX(pt.getX)
    val j = snapY(pt.getY)
    buckets(i)(j).put(key, value)
  }

  override def insert(envelope: Envelope, key: String, item: T): Unit = {
    if (envelope.getArea > 0) {
      logger.warn(s"This index only supports point inserts, but received $envelope - will insert using the centroid")
    }
    insert((envelope.getMinX + envelope.getMaxX) / 2.0, (envelope.getMinY + envelope.getMaxY) / 2.0, key, item)
  }

  override def remove(geom: Geometry, key: String): T = {
    val pt = geom.asInstanceOf[Point]
    val i = snapX(pt.getX)
    val j = snapY(pt.getY)
    buckets(i)(j).remove(key)
  }

  override def remove(envelope: Envelope, key: String): T =
    remove((envelope.getMinX + envelope.getMaxX) / 2.0, (envelope.getMinY + envelope.getMaxY) / 2.0, key)

  override def get(geom: Geometry, key: String): T = {
    val pt = geom.asInstanceOf[Point]
    val i = snapX(pt.getX)
    val j = snapY(pt.getY)
    buckets(i)(j).get(key)
  }

  override def get(envelope: Envelope, key: String): T =
    get((envelope.getMinX + envelope.getMaxX) / 2.0, (envelope.getMinY + envelope.getMaxY) / 2.0, key)

  override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T] =
    new BucketIterator(snapX(xmin), snapX(xmax), snapY(ymin), snapY(ymax))

  override def query(): Iterator[T] = new BucketIterator(0, xBuckets - 1, 0, yBuckets - 1)

  override def size(): Int = {
    var size = 0
    var i = 0
    while (i < xBuckets) {
      var j = 0
      while (j < yBuckets) {
        size += buckets(i)(j).size()
        j += 1
      }
      i += 1
    }
    size
  }

  override def clear(): Unit = {
    var i = 0
    while (i < xBuckets) {
      var j = 0
      while (j < yBuckets) {
        buckets(i)(j).clear()
        j += 1
      }
      i += 1
    }
  }

  private def snapX(x: Double): Int = {
    val i = gridSnap.i(x)
    if (i != -1) { i } else if (x < extents.getMinX) { 0 } else { xBuckets - 1 }
  }

  private def snapY(y: Double): Int = {
    val j = gridSnap.j(y)
    if (j != -1) { j } else if (y < extents.getMinY) { 0 } else { yBuckets - 1 }
  }

  /**
    * Iterator over a range of buckets
    */
  class BucketIterator private [BucketIndex] (mini: Int, maxi: Int, minj: Int, maxj: Int) extends Iterator[T] {

    private var i = mini
    private var j = minj
    private var iter = buckets(i)(j).values.iterator() // note: `.values` is a cached view

    @tailrec
    override final def hasNext: Boolean = iter.hasNext || {
      if (i == maxi && j == maxj) { false } else {
        if (j < maxj) {
          j += 1
        } else {
          j = minj
          i += 1
        }
        iter = buckets(i)(j).values.iterator() // note: `.values` is a cached view
        hasNext
      }
    }

    override def next(): T = iter.next()
  }
}
