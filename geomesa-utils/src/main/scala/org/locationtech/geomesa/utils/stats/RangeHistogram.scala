/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.reflect.ClassTag

/**
 * The range histogram's state is stored in an indexed array, where the index is the bin number
 * and the values are the counts.
 *
 * e.g. a range of 0 to 3 with 3 bins will result in these bins: [0, 1), [1, 2), [2, 3) and the
 * array will contain three entries.
 *
 * @param attribute attribute index for the attribute the histogram is being made for
 * @param initialBins number of bins the histogram has
 * @param initialEndpoints lower/upper end of histogram
 * @tparam T a comparable type which must have a StatHelperFunctions type class
 */
class RangeHistogram[T](val attribute: Int, initialBins: Int, initialEndpoints: (T, T))
                       (implicit defaults: MinMax.MinMaxDefaults[T], ct: ClassTag[T]) extends Stat {

  override type S = RangeHistogram[T]

  private [stats] var bins: BinnedArray[T] = BinnedArray[T](initialBins, initialEndpoints)
  lazy private val stringify = Stat.stringifier(ct.runtimeClass, json = true)

  def length: Int = bins.length
  def indexOf(value: T): Int = bins.indexOf(value)
  def count(i: Int): Long = bins.counts(i)
  def endpoints: (T, T) = bins.bounds
  def medianValue(i: Int): T = bins.medianValue(i)

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute)
    if (value != null) {
      bins.add(value.asInstanceOf[T])
    }
  }

  override def +(other: RangeHistogram[T]): RangeHistogram[T] = {
    if (length == other.length && endpoints == other.endpoints) {
      // hists match - we can just copy counts in directly
      val plus = new RangeHistogram(attribute, length, endpoints)
      var i = 0
      while (i < plus.bins.length) {
        plus.bins.counts(i) += (bins.counts(i) + other.bins.counts(i))
        i += 1
      }
      plus
    } else {
      // create the bin array at it's most expansive up front so that
      // we don't have to re-create it in plus-equals
      val maxEndpoints = RangeHistogram.expandEndpoints(endpoints, other.endpoints)
      val maxLength = math.max(length, other.length)
      val plus = new RangeHistogram(attribute, maxLength, maxEndpoints)
      plus += this
      plus += other
      plus
    }
  }

  override def +=(other: RangeHistogram[T]): Unit = {
    if (length == other.length && endpoints == other.endpoints) {
      // hists match - we can just copy counts in directly
      var i = 0
      while (i < bins.length) {
        bins.counts(i) += other.bins.counts(i)
        i += 1
      }
    } else {
      // figure out the new bounds and size - expand to use the widest option
      val newEndpoints = RangeHistogram.expandEndpoints(endpoints, other.endpoints)
      val newLength = math.max(length, other.length)
      if (newEndpoints != endpoints || newLength != length) {
        // if the other hist was not 'contained' in this one, we have to re-create the bins
        val newBins = BinnedArray(newLength, newEndpoints)
        RangeHistogram.copyInto(newBins, bins)
        bins = newBins
      }
      // now copy over the other bins
      RangeHistogram.copyInto(bins, other.bins)
    }
  }

  override def toJson: String =
    s"""{ "lower-bound" : ${stringify(endpoints._1)}, "upper-bound" : ${stringify(endpoints._2)},""" +
        s""""bins" : [ ${bins.counts.mkString(", ")} ] }"""

  override def isEmpty: Boolean = bins.counts.forall(_ == 0)

  override def clear(): Unit = bins.clear()

  override def equals(other: Any): Boolean = other match {
    case that: RangeHistogram[T] => attribute == that.attribute && bins == that.bins // bins compares endpoints and length
    case _ => false
  }

  override def hashCode(): Int = Seq(attribute, bins).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
}

object RangeHistogram {

  /**
    * Creates expanded endpoints that cover both input endpoints
    */
  def expandEndpoints[T](one: (T, T), two: (T, T))(implicit defaults: MinMax.MinMaxDefaults[T]): (T, T) = {
    val leftExpand = defaults.minmax(two._1, one._1, one._2)
    defaults.minmax(two._2, leftExpand._1, leftExpand._2)
  }

  /**
    * Copies data from one binned array into the other. Arrays are assumed to have different
    * sizes and/or endpoints. If arrays have the same characteristics, this method is
    * needlessly expensive/complicated/inexact.
    */
  def copyInto[T](to: BinnedArray[T], from: BinnedArray[T]): Unit = {
    var i = 0
    while (i < from.length) {
      val count = from.counts(i)
      if (count > 0) {
        val (min, max) = from.bounds(i)
        val (lo, hi) = (to.indexOf(min), to.indexOf(max))
        if (lo == hi) {
          to.counts(lo) += count
        } else {
          val size = hi - lo + 1
          val avgCount = count / size
          val remainingCount = count % size
          val mid = lo + (size / 2)
          var j = lo
          while (j <= hi) {
            to.counts(j) += avgCount
            if (j == mid) {
              to.counts(j) += remainingCount
            }
            j += 1
          }
        }
      }
      i += 1
    }
  }
}