/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.StrictLogging
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.geomesa.utils.geotools.GridSnap

import scala.annotation.tailrec

/**
  * Creates an index suitable for geometries with extents. The index is broken up into tiers, where each
  * geometry is assigned a tier based on its envelope size. When querying, each tier must be evaluated, so
  * if possible tiers should be matched closely to the envelopes of the entries.
  *
  * Values are indexed by the centroid of their envelope. When querying, the bounding box is expanded based
  * on the max envelope size of the tier, to ensure that all potential results are found. Thus, the false
  * positive rate tends to go up with larger tiers, and post-filtering is recommended.
  *
  * @param sizes (x, y) max envelope size for each tier
  * @param xBucketMultiplier multiplier for number of x buckets to create per tier
  * @param yBucketMultiplier multiplier for number of y buckets to create per tier
  * @param extents total area being indexed
  * @tparam T item type
  */
class SizeSeparatedBucketIndex[T](sizes: Seq[(Double, Double)] = SizeSeparatedBucketIndex.DefaultTiers,
                                  xBucketMultiplier: Double = 1,
                                  yBucketMultiplier: Double = 1,
                                  extents: Envelope = new Envelope(-180.0, 180.0, -90.0, 90.0))
    extends SpatialIndex[T] with StrictLogging {

  // TODO https://geomesa.atlassian.net/browse/GEOMESA-2323 better anti-meridian handling

  require(sizes.nonEmpty, "No valid tier sizes specified")
  require(sizes.lengthCompare(1) == 0 ||
      sizes.sliding(2).forall { case Seq((x1, y1), (x2, y2)) => x1 <= x2 && y1 <= y2 },
    "Tiers must be ordered by increasing size")

  // note: for point ops, we always use the first (smallest) tier

  private val tiers = sizes.map { case (width, height) =>
    val xSize = math.ceil(extents.getWidth * xBucketMultiplier / width).toInt
    val ySize = math.ceil(extents.getHeight * yBucketMultiplier / height).toInt
    // create the buckets up front to avoid having to synchronize the whole array
    // we use a ConcurrentHashMap, which gives us iterators that aren't affected by modifications to the backing map
    val buckets = Array.fill(xSize, ySize)(new ConcurrentHashMap[String, T]())
    logger.debug(s"Creating tier for size ($width $height) with buckets [${xSize}x$ySize]")
    new Tier(width, height, buckets, new GridSnap(extents, xSize, ySize))
  }


  override def insert(geom: Geometry, key: String, value: T): Unit = {
    val envelope = geom.getEnvelopeInternal
    val tier = selectTier(envelope)
    // volatile reads should be cheaper than writes, so only update the variable if necessary
    if (tier.empty) {
      tier.empty = false
    }
    tier.bucket(envelope).put(key, value)
  }

  override def remove(geom: Geometry, key: String): T = {
    val envelope = geom.getEnvelopeInternal
    selectTier(envelope).bucket(envelope).remove(key)
  }

  override def get(geom: Geometry, key: String): T = {
    val envelope = geom.getEnvelopeInternal
    selectTier(envelope).bucket(envelope).get(key)
  }

  override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T] =
    tiers.iterator.flatMap(_.iterator(xmin, ymin, xmax, ymax))

  override def query(): Iterator[T] = query(extents.getMinX, extents.getMinY, extents.getMaxX, extents.getMaxY)

  override def size(): Int = {
    var size = 0
    tiers.foreach(tier => size += tier.size())
    size
  }

  override def clear(): Unit = tiers.foreach(_.clear())

  private def selectTier(envelope: Envelope): Tier = {
    val width = envelope.getWidth
    val height = envelope.getHeight
    tiers.find(t => t.maxSizeX >= width && t.maxSizeY >= height).getOrElse {
      throw new IllegalArgumentException(s"Envelope $envelope exceeds the max tier size ${sizes.last}")
    }
  }

  private class Tier(val maxSizeX: Double,
                     val maxSizeY: Double,
                     buckets: Array[Array[ConcurrentHashMap[String, T]]],
                     gridSnap: GridSnap) {

    // we can safely use volatile instead of synchronized here, as this is a primitive boolean whose
    // state doesn't depend on its own value
    @volatile
    var empty: Boolean = true

    private val maxX = buckets.length - 1
    private val maxY = buckets(0).length - 1

    def bucket(x: Double, y: Double): ConcurrentHashMap[String, T] = buckets(snapX(x))(snapY(y))

    // the bucket is selected based on the envelope centroid
    def bucket(envelope: Envelope): ConcurrentHashMap[String, T] =
      buckets(snapX((envelope.getMinX + envelope.getMaxX) / 2.0))(snapY((envelope.getMinY + envelope.getMaxY) / 2.0))

    def iterator(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T] =
      if (empty) { Iterator.empty } else { new TierIterator(xmin, ymin, xmax, ymax) }

    def size(): Int = {
      if (empty) { 0 } else {
        var size = 0
        var i = 0
        while (i <= maxX) {
          var j = 0
          while (j <= maxY) {
            size += buckets(i)(j).size()
            j += 1
          }
          i += 1
        }
        size
      }
    }

    def clear(): Unit = {
      var i = 0
      while (i <= maxX) {
        var j = 0
        while (j <= maxY) {
          buckets(i)(j).clear()
          j += 1
        }
        i += 1
      }
    }

    private def snapX(x: Double): Int = {
      val i = gridSnap.i(x)
      if (i != -1) { i } else if (x < extents.getMinX) { 0 } else { maxX }
    }

    private def snapY(y: Double): Int = {
      val j = gridSnap.j(y)
      if (j != -1) { j } else if (y < extents.getMinY) { 0 } else { maxY }
    }

    /**
      * Iterator over a range of buckets
      */
    class TierIterator (xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends Iterator[T] {

      private val maxi = snapX(xmax + maxSizeX)
      private val minj = snapY(ymin - maxSizeY)
      private val maxj = snapY(ymax + maxSizeY)

      private var i = snapX(xmin - maxSizeX)
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
}

object SizeSeparatedBucketIndex {
  // TODO https://geomesa.atlassian.net/browse/GEOMESA-2322 these are somewhat arbitrary
  val DefaultTiers: Seq[(Double, Double)] = Seq((1, 1), (4, 4), (32, 32), (360, 180))
}
