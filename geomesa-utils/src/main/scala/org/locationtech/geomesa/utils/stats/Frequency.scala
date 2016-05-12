/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.{Date, Locale}

import com.clearspring.analytics.stream.frequency.{CountMinSketch, IFrequency, RichCountMinSketch}
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeature

import scala.reflect.ClassTag

/**
  *  Estimates frequency counts at scale
  *
  * @param attribute attribute index for the attribute the sketch is being made for
  * @param eps (epsilon) with probability at least @see confidence, estimates will be within eps * N
  * @param confidence percent - with probability at least confidence, estimates will be within @see eps * N
  * @param precision for geometry types, this is the number of bits of z-index to keep (max of 64)
  *                  (note: first 2 bits do not hold any info)
  *                  for date types, this is the number of milliseconds to group for binning
  *                  for number types, this is the number of digits that will be grouped together
  *                  for floating point types, this is the number of decimal places that will be considered
  *                  for string types, this is the number of characters that will be considered
  * @param ct class tag
  * @tparam T type parameter, should match the type binding of the attribute
  */
class Frequency[T](val attribute: Int,
                   val precision: Int,
                   val eps: Double = 0.005,
                   val confidence: Double = 0.95)(implicit ct: ClassTag[T]) extends Stat {

  override type S = Frequency[T]

  private [stats] val sketch: CountMinSketch = new CountMinSketch(eps, confidence, Frequency.Seed)

  private val addAttribute = Frequency.add[T](ct.runtimeClass.asInstanceOf[Class[T]], precision)
  private val getCount = Frequency.count[T](ct.runtimeClass.asInstanceOf[Class[T]], precision)

  /**
    * Gets the count for a given value
    *
    * @param value value to consider
    * @return count of the value
    */
  def count(value: T): Long = getCount(sketch, value)

  /**
    * Gets the count for a given value, which has already been converted into a string. Useful
    * if you know the string key space ahead of time.
    *
    * @param value value to consider, converted into an appropriate string key
    * @return count of the value
    */
  def countDirect(value: String): Long = sketch.estimateCount(value)

  /**
    * Gets the count for a given value, which has already been converted into a long. Useful
    * if you know the long key space ahead of time (e.g. with z-values).
    *
    * @param value value to consider, converted into an appropriate long key
    * @return count of the value
    */
  def countDirect(value: Long): Long = sketch.estimateCount(value)

  /**
    * Number of observations in the frequency map
    *
    * @return number of observations
    */
  def size: Long = sketch.size()

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute)
    if (value != null) {
      addAttribute(sketch, value.asInstanceOf[T])
    }
  }

  // no-op
  override def unobserve(sf: SimpleFeature): Unit = {}

  override def +(other: Frequency[T]): Frequency[T] = {
    val plus = new Frequency[T](attribute, precision, sketch.getRelativeError, sketch.getConfidence)
    plus += this
    plus += other
    plus
  }

  override def +=(other: Frequency[T]): Unit = new RichCountMinSketch(sketch).add(other.sketch)

  override def clear(): Unit = new RichCountMinSketch(sketch).clear()

  override def isEmpty: Boolean = sketch.size == 0

  override def toJson: String = {
    val r = new RichCountMinSketch(sketch)
    s"{ width : ${r.width}, depth : ${r.depth}, size : ${sketch.size} }"
  }

  override def isEquivalent(other: Stat): Boolean = {
    other match {
      case s: Frequency[T] =>
        attribute == s.attribute && precision == s.precision &&
            new RichCountMinSketch(sketch).isEquivalent(s.sketch)
      case _ => false
    }
  }
}

object Frequency {

  // the seed for our frequencies - frequencies can only be combined if they have the same seed.
  val Seed = -27

  /**
    * Enumerate all the values contained in a sequence of ranges, using the supplied precision.
    * Because frequency can only do point lookups, this can be used to convert a range into a sequence
    * of points.
    *
    * For example, [1, 4] would be converted into Seq(1, 2, 3, 4)
    *
    * @param ranges ranges to enumerate
    * @param precision precision of the ranges, in bits [1, 64]
    * @return the enumerated values
    */
  def enumerate(ranges: Seq[IndexRange], precision: Long): Iterator[Long] = {
    val shift = 64 - precision
    ranges.toIterator.flatMap { r =>
      val c = (r.upper >> shift) - (r.lower >> shift)
      new Iterator[Long] {
        var i = 0L
        override def hasNext: Boolean = i <= c
        override def next(): Long = try { r.lower + (i << shift) } finally { i += 1 }
      }
    }
  }

  def add[T](clas: Class[T], precision: Int): (IFrequency, T) => Unit = {
    if (classOf[Geometry].isAssignableFrom(clas)) {
      val mask = getMask(precision)
      (sketch, value) => sketch.add(geomToKey(value.asInstanceOf[Geometry], mask), 1L)
    } else if (classOf[Date].isAssignableFrom(clas)) {
      (sketch, value) => sketch.add(dateToKey(value.asInstanceOf[Date], precision), 1L)
    } else if (clas == classOf[String]) {
      (sketch, value) => sketch.add(stringToKey(value.asInstanceOf[String], precision), 1L)
    } else if (clas == classOf[java.lang.Long]) {
      (sketch, value) => sketch.add(longToKey(value.asInstanceOf[Long], precision), 1L)
    } else if (clas == classOf[Integer]) {
      (sketch, value) => sketch.add(intToKey(value.asInstanceOf[Int], precision), 1L)
    } else if (clas == classOf[java.lang.Float]) {
      (sketch, value) => sketch.add(floatToKey(value.asInstanceOf[Float], precision), 1L)
    } else if (clas == classOf[java.lang.Double]) {
      (sketch, value) => sketch.add(doubleToKey(value.asInstanceOf[Double], precision), 1L)
    } else {
      throw new IllegalArgumentException(s"No CountMinSketch implementation for class binding ${clas.getName}")
    }
  }

  def count[T](clas: Class[T], precision: Int): (IFrequency, T) => Long = {
    if (classOf[Geometry].isAssignableFrom(clas)) {
      val mask = getMask(precision)
      (sketch, value) => sketch.estimateCount(geomToKey(value.asInstanceOf[Geometry], mask))
    } else if (classOf[Date].isAssignableFrom(clas)) {
      (sketch, value) => sketch.estimateCount(dateToKey(value.asInstanceOf[Date], precision))
    } else if (clas == classOf[String]) {
      (sketch, value) => sketch.estimateCount(stringToKey(value.asInstanceOf[String], precision))
    } else if (clas == classOf[java.lang.Long]) {
      (sketch, value) => sketch.estimateCount(longToKey(value.asInstanceOf[Long], precision))
    } else if (clas == classOf[Integer]) {
      (sketch, value) => sketch.estimateCount(intToKey(value.asInstanceOf[Int], precision))
    } else if (clas == classOf[java.lang.Float]) {
      (sketch, value) => sketch.estimateCount(floatToKey(value.asInstanceOf[Float], precision))
    } else if (clas == classOf[java.lang.Double]) {
      (sketch, value) => sketch.estimateCount(doubleToKey(value.asInstanceOf[Double], precision))
    } else {
      throw new IllegalArgumentException(s"No CountMinSketch implementation for class binding ${clas.getName}")
    }
  }

  // mask for right-zeroing bits
  private [stats] def getMask(precision: Int): Long = {
    require(precision >= 0 && precision <= 64, "Precision must be in the range [0, 64]")
    Long.MaxValue << (64 - precision)
  }

  private [stats] def geomToKey(value: Geometry, mask: Long): Long = {
    val centroid = value.getCentroid
    Z2SFC.index(centroid.getX, centroid.getY).z & mask
  }

  private [stats] def stringToKey(value: String, precision: Int): String = {
    if (value.length > precision) {
      value.substring(0, precision).toLowerCase(Locale.US)
    } else {
      value.toLowerCase(Locale.US)
    }
  }
  private [stats] def dateToKey(value: Date, precision: Int): Long = value.getTime / precision
  private [stats] def longToKey(value: Long, precision: Int): Long = value / precision
  private [stats] def intToKey(value: Int, precision: Int): Long = value / precision
  private [stats] def floatToKey(value: Float, precision: Int): Long = math.round(value * precision)
  private [stats] def doubleToKey(value: Double, precision: Int): Long = math.round(value * precision)
}
