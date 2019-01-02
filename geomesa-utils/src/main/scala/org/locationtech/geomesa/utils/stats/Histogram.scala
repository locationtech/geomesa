/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

/**
  * The histogram's state is stored in an indexed array, where the index is the bin number
  * and the values are the counts.
  *
  * e.g. a range of 0 to 3 with 3 bins will result in these bins: [0, 1), [1, 2), [2, 3) and the
  * array will contain three entries.
  *
  * @param sft simple feature type
  * @param property property name for the attribute the histogram is being made for
  * @param initialBins number of bins the histogram has
  * @param initialEndpoints lower/upper end of histogram
  * @tparam T a comparable type which must have a StatHelperFunctions type class
  */
class Histogram[T] private [stats] (val sft: SimpleFeatureType,
                                    val property: String,
                                    initialBins: Int,
                                    initialEndpoints: (T, T))
                                   (implicit val defaults: MinMax.MinMaxDefaults[T],
                                    ct: ClassTag[T]) extends Stat with LazyLogging {

  override type S = Histogram[T]

  @deprecated("property")
  lazy val attribute: Int = i

  private val i = sft.indexOf(property)
  private [stats] var bins: BinnedArray[T] = BinnedArray[T](initialBins, initialEndpoints)

  def length: Int = bins.length
  def directIndex(value: Long): Int = bins.directIndex(value)
  def indexOf(value: T): Int = bins.indexOf(value)
  def count(i: Int): Long = bins.counts(i)
  def min: T = bins.bounds._1
  def max: T = bins.bounds._2
  def bounds: (T, T) = bins.bounds
  def bounds(i: Int): (T, T) = bins.bounds(i)
  def medianValue(i: Int): T = bins.medianValue(i)

  /**
    * Copies another histogram into this one. In comparison to +=, this method will preserve the
    * current bounds and length.
    *
    * @param other other histogram
    */
  def addCountsFrom(other: Histogram[T]): Unit = {
    if (length == other.length && bounds == other.bounds) {
      this += other
    } else {
      Histogram.copyInto(bins, other.bins)
    }
  }

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(i)
    if (value != null) {
      try {
        val i = bins.indexOf(value.asInstanceOf[T])
        if (i == -1) {
          bins = Histogram.expandBins(value.asInstanceOf[T], bins)
          bins.add(value.asInstanceOf[T])
        } else {
          bins.counts(i) += 1
        }
      } catch {
        case e: Exception => logger.warn(s"Error observing value '$value': ${e.toString}")
      }
    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(i)
    if (value != null) {
      try {
        val i = bins.indexOf(value.asInstanceOf[T])
        if (i == -1) {
          bins = Histogram.expandBins(value.asInstanceOf[T], bins)
          bins.add(value.asInstanceOf[T], -1)
        } else {
          bins.counts(i) -= 1
        }
      } catch {
        case e: Exception => logger.warn(s"Error un-observing value '$value': ${e.toString}")
      }
    }
  }

  /**
    * Creates a new histogram by combining another histogram with this one.
    * Bounds and length will both be the greater from each histogram.
    */
  override def +(other: Histogram[T]): Histogram[T] = {
    val plus = new Histogram(sft, property, length, bounds)
    plus += this
    plus += other
    plus
  }

  /**
    * Copies another histogram into this one.
    * Current bounds and length will be expanded if necessary.
    */
  override def +=(other: Histogram[T]): Unit = {
    if (length == other.length && bounds == other.bounds) {
      // hists match - we can just copy counts in directly
      var i = 0
      while (i < bins.length) {
        bins.counts(i) += other.bins.counts(i)
        i += 1
      }
    } else if (other.isEmpty) {
      // no-op
    } else if (isEmpty) {
      // copy the data from the other histogram
      bins = BinnedArray(other.length, other.bounds)
      var i = 0
      while (i < bins.length) {
        bins.counts(i) = other.bins.counts(i)
        i += 1
      }
    } else {
      // figure out the new bounds and size
      val newEndpoints = Histogram.checkEndpoints(bins, other.bins)
      val newLength = math.max(length, other.length)
      if (newEndpoints != bounds || newLength != length) {
        // if the other hist was not 'contained' in this one, we have to re-create the bins
        val newBins = BinnedArray(newLength, newEndpoints)
        Histogram.copyInto(newBins, bins)
        bins = newBins
      }
      // now copy over the other bins
      Histogram.copyInto(bins, other.bins)
    }
  }

  override def toJsonObject: Map[String, Any] = {
    val binSeq = Seq.tabulate(bins.length) { bin =>
      val builder = ListMap.newBuilder[String, Any]
      builder.sizeHint(if (bin == 0) { 4 } else { 3 })
      builder += "index" -> bin
      builder += "lower-bound" -> bounds(bin)._1
      if (bin == 0) {
        builder += "upper-bound" -> bounds(bin)._2
      }
      builder += "count" -> bins.counts(bin)
      builder.result
    }
    ListMap("lower-bound" -> bounds._1, "upper-bound" -> bounds._2, "bins" -> binSeq)

  }

  override def isEmpty: Boolean = bins.counts.forall(_ == 0)

  override def clear(): Unit = bins.clear()

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: Histogram[T] =>
      property == that.property && bounds == that.bounds &&
          java.util.Arrays.equals(bins.counts, that.bins.counts)
    case _ => false
  }
}

object Histogram {

  /**
    * Takes a single value and creates a range from it. Since the range histogram lower bound has to be
    * strictly less than the upper bound, this lets us use a single value to create a valid histogram.
    *
    * @param value value to buffer
    * @tparam T type of the value
    * @return valid bounds for a histogram
    */
  def buffer[T](value: T): (T, T) = {
    import BinnedStringArray.{Base36Lowest, Base36Highest}
    val buf = value match {
      case v: Int    => (v - 100, v + 100)
      case v: Long   => (v - 100, v + 100)
      case v: Float  => (v - 100, v + 100)
      case v: Double => (v - 100, v + 100)
      case v: String => (s"$v$Base36Lowest", s"$v$Base36Highest")
      case v: Date   => (new Date(v.getTime - 60000), new Date(v.getTime + 60000))
      case v: Geometry =>
        import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
        val env = v.safeCentroid().buffer(10.0).getEnvelopeInternal
        val min = GeometryUtils.geoFactory.createPoint(new Coordinate(env.getMinX, env.getMinY))
        val max = GeometryUtils.geoFactory.createPoint(new Coordinate(env.getMaxX, env.getMaxY))
        (min, max)
    }
    buf.asInstanceOf[(T, T)]
  }

  /**
    * Creates a new binned array that encompasses the new value.
    *
    * Assumes that the value is not already within the bounds for the existing binned array.
    */
  def expandBins[T](value: T, old: BinnedArray[T])(implicit defaults: MinMax.MinMaxDefaults[T],ct: ClassTag[T]): BinnedArray[T] = {
    val min = defaults.min(value, old.bounds._1)
    val max = defaults.max(value, old.bounds._2)
    val bins = BinnedArray[T](old.length, (min, max))
    copyInto(bins, old)
    bins
  }

  /**
    * Gets new endpoints that encompass both arrays. If either array has empty values to start or end, the
    * bounds will be trimmed down to be the start/end of non-empty values.
    */
  def checkEndpoints[T](left: BinnedArray[T], right: BinnedArray[T])(implicit defaults: MinMax.MinMaxDefaults[T]): (T, T) = {
    val (lMin, lMax) = getActualBounds(left)
    val (rMin, rMax) = getActualBounds(right)
    (defaults.min(lMin, rMin), defaults.max(lMax, rMax))
  }

  /**
    * Gets the bounds of the array that actually contain values.
    */
  private def getActualBounds[T](bins: BinnedArray[T]): (T, T) = {
    val minIndex = bins.counts.indexWhere(_ != 0)
    val maxIndex = bins.counts.length - bins.counts.reverse.indexWhere(_ != 0) - 1
    val min = if (minIndex <= 0) bins.bounds._1 else bins.bounds(minIndex)._1
    val max = if (maxIndex >= bins.counts.length -1) bins.bounds._2 else bins.bounds(maxIndex)._2
    (min, max)
  }

  /**
    * Copies data from one binned array into the other. Arrays are assumed to have different
    * sizes and/or endpoints. If arrays have the same characteristics, this method is
    * needlessly expensive/complicated/inexact.
    */
  def copyInto[T](to: BinnedArray[T], from: BinnedArray[T]): Unit = {
    def toIndex(value: T): Int = {
      val i = to.indexOf(value)
      if (i != -1) i else if (to.isBelow(value)) 0 else to.length - 1
    }

    var i = 0
    while (i < from.length) {
      val count = from.counts(i)
      if (count > 0) {
        val (min, max) = from.bounds(i)
        val lo = toIndex(min)
        val hi = toIndex(max)
        if (lo == hi) {
          to.counts(lo) += count
        } else {
          val size = hi - lo + 1
          require(size > 0,
            s"Error calculating bounds for ${min.getClass.getSimpleName} from ${from.bounds} to ${to.bounds}")
          val avgCount = count / size
          val remainingCount = count % size
          val mid = lo + (size / 2)
          var j = lo
          while (j <= hi) {
            to.counts(j) += avgCount
            j += 1
          }
          to.counts(mid) += remainingCount
        }
      }
      i += 1
    }
  }
}