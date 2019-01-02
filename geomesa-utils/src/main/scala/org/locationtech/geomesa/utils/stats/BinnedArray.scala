/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.{Date, Locale}

import org.locationtech.jts.geom.{Coordinate, Geometry, Point}
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.sfcurve.zorder.Z2

import scala.reflect.ClassTag

/**
  * Puts inputs into sorted bins and stores count of each bin
  *
  * @param length number of bins
  * @param bounds upper and lower bounds for the input values
  * @tparam T type of input value
  */
abstract class BinnedArray[T](val length: Int, val bounds: (T, T)) {

  private [stats] val counts = Array.fill[Long](length)(0L)

  /**
    * Gets the count of entries in the given bin
    *
    * @param index bin index
    * @return count
    */
  def apply(index: Int): Long = counts(index)

  /**
    * Clears the counts
    */
  def clear(): Unit = {
    var i = 0
    while (i < length) {
      counts(i) = 0L
      i +=1
    }
  }

  /**
    * Increment the count for the bin corresponding to this value
    *
    * @param value value
    * @param count how much to increment
    */
  def add(value: T, count: Long = 1): Unit = {
    val i = indexOf(value)
    if (i != -1) {
      counts(i) += count
    }
  }

  /**
    * Maps a value that has already been transformed into a number to a bin index.
    *
    * @param value value
    * @return bin index, or -1 if value is out of bounds
    */
  def directIndex(value: Long): Int

  /**
    * Maps a value to a bin index.
    *
    * @param value value
    * @return bin index, or -1 if value is out of bounds
    */
  def indexOf(value: T): Int

  /**
    * Gets a value corresponding to the midpoint of a bin.
    *
    * @param index index into the array
    * @return representative value for the bin
    */
  def medianValue(index: Int): T

  /**
    * Gets the min and max values that will go into a bin
    *
    * @param index index into the array
    * @return bounds for the bin
    */
  def bounds(index: Int): (T, T)

  /**
    * Indicates if the value is below the range of this array
    *
    * @param value value
    * @return true if below, false otherwise (implies above if indexOf == -1)
    */
  def isBelow(value: T): Boolean
}

object BinnedArray {
  def apply[T](length: Int, bounds: (T, T))(implicit c: ClassTag[T]): BinnedArray[T] = {
    val clas = c.runtimeClass
    val ba = if (clas == classOf[String]) {
      new BinnedStringArray(length, bounds.asInstanceOf[(String, String)])
    } else if (clas == classOf[Integer]) {
      new BinnedIntegerArray(length, bounds.asInstanceOf[(Integer, Integer)])
    } else if (clas == classOf[jLong]) {
      new BinnedLongArray(length, bounds.asInstanceOf[(jLong, jLong)])
    } else if (clas == classOf[jFloat]) {
      new BinnedFloatArray(length, bounds.asInstanceOf[(jFloat, jFloat)])
    } else if (clas == classOf[jDouble]) {
      new BinnedDoubleArray(length, bounds.asInstanceOf[(jDouble, jDouble)])
    } else if (classOf[Date].isAssignableFrom(clas)) {
      new BinnedDateArray(length, bounds.asInstanceOf[(Date, Date)])
    } else if (classOf[Geometry].isAssignableFrom(clas)) {
      new BinnedGeometryArray(length, bounds.asInstanceOf[(Geometry, Geometry)])
    } else {
      throw new UnsupportedOperationException(s"BinnedArray not implemented for ${clas.getName}")
    }
    ba.asInstanceOf[BinnedArray[T]]
  }
}

abstract class WholeNumberBinnedArray[T](length: Int, bounds: (T, T)) extends BinnedArray[T](length, bounds) {

  private val min = convertToLong(bounds._1)
  private val max = convertToLong(bounds._2)

  require(min < max,
    s"Upper bound must be greater than lower bound: lower='${bounds._1}'($min) upper='${bounds._2}'($max)")

  private val binSize = (max - min).toDouble / length

  override def directIndex(value: Long): Int = {
    if (value < min || value > max) { -1 } else {
      val i = math.floor((value - min) / binSize).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
    }
  }

  override def indexOf(value: T): Int = directIndex(convertToLong(value))

  override def medianValue(index: Int): T = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val long = min + math.round(binSize / 2 + binSize * index)
    if (long > max) bounds._2 else convertFromLong(long)
  }

  override def bounds(index: Int): (T, T) = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val loLong = min + math.ceil(binSize * index).toLong
    val hiLong = math.max(loLong, min + math.floor(binSize * (index + 1)).toLong)
    val lo = if (loLong <= min) bounds._1 else convertFromLong(loLong)
    val hi = if (hiLong >= max) bounds._2 else convertFromLong(hiLong)

    (lo, hi)
  }

  override def isBelow(value: T): Boolean = convertToLong(value) < min

  /**
    * Maps a value to a long used to allocate values in bins
    *
    * @param value value to convert
    * @return value as a long
    */
  protected def convertToLong(value: T): Long

  /**
    * Maps a long back to a value
    *
    * @param value value as a long
    * @return value
    */
  protected def convertFromLong(value: Long): T
}

class BinnedIntegerArray(length: Int, bounds: (Integer, Integer))
    extends WholeNumberBinnedArray[Integer](length, bounds) {
  override protected def convertToLong(value: Integer): Long = value.toLong
  override protected def convertFromLong(value: Long): Integer = value.toInt
}

class BinnedLongArray(length: Int, bounds: (jLong, jLong))
    extends WholeNumberBinnedArray[jLong](length, bounds) {
  override protected def convertToLong(value: jLong): Long = value
  override protected def convertFromLong(value: Long): jLong = value
}

class BinnedDateArray(length: Int, bounds: (Date, Date))
    extends WholeNumberBinnedArray[Date](length, bounds) {
  override protected def convertToLong(value: Date): Long = value.getTime
  override protected def convertFromLong(value: Long): Date = new Date(value)
}

/**
  * Sorts geometries based on the z-value of their centroid
  *
  * @param length number of bins
  * @param bounds upper and lower bounds for the input values
  */
class BinnedGeometryArray(length: Int, bounds: (Geometry, Geometry))
    extends WholeNumberBinnedArray[Geometry](length, bounds) {

  override protected def convertToLong(value: Geometry): Long = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    val centroid = value match {
      case p: Point => p
      case g => g.safeCentroid()
    }
    Z2SFC.index(centroid.getX, centroid.getY, lenient = true).z
  }

  override protected def convertFromLong(value: Long): Geometry = {
    val (x, y) = Z2SFC.invert(new Z2(value))
    GeometryUtils.geoFactory.createPoint(new Coordinate(x, y))
  }
}

class BinnedFloatArray(length: Int, bounds: (jFloat, jFloat)) extends BinnedArray[jFloat](length, bounds) {

  require(bounds._1 < bounds._2,
    s"Upper bound must be greater than lower bound: lower='${bounds._1}' upper='${bounds._2}'")

  private val binSize = (bounds._2 - bounds._1) / length

  override def directIndex(value: Long): Int = -1

  override def indexOf(value: jFloat): Int = {
    if (value < bounds._1 || value > bounds._2) { -1 } else {
      val i = math.floor((value - bounds._1) / binSize).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
    }
  }

  override def medianValue(index: Int): jFloat = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    bounds._1 + binSize / 2 + binSize * index
  }

  override def bounds(index: Int): (jFloat, jFloat) = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    (bounds._1 + binSize * index, bounds._1 + binSize * (index + 1))
  }

  override def isBelow(value: jFloat): Boolean = value < bounds._1
}

class BinnedDoubleArray(length: Int, bounds: (jDouble, jDouble)) extends BinnedArray[jDouble](length, bounds) {

  require(bounds._1 < bounds._2,
    s"Upper bound must be greater than lower bound: lower='${bounds._1}' upper='${bounds._2}'")

  private val binSize = (bounds._2 - bounds._1) / length

  override def directIndex(value: Long): Int = -1

  override def indexOf(value: jDouble): Int = {
    if (value < bounds._1 || value > bounds._2) { -1 } else {
      val i = math.floor((value - bounds._1) / binSize).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
    }
  }

  override def medianValue(index: Int): jDouble = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    bounds._1 + binSize / 2 + binSize * index
  }

  override def bounds(index: Int): (jDouble, jDouble) = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    (bounds._1 + binSize * index, bounds._1 + binSize * (index + 1))
  }

  override def isBelow(value: jDouble): Boolean = value < bounds._1
}

/**
  * Bins strings. Will set up bins based on the longest-common-prefix of the bounds. Estimates
  * bins by considering inputs to be roughly equivalent to base36 longs.
  *
  * @param length number of bins
  * @param rawBounds upper and lower bounds for the input values
  */
class BinnedStringArray(length: Int, rawBounds: (String, String))
    extends WholeNumberBinnedArray[String](length, BinnedStringArray.normalizeBounds(rawBounds)) {

  import BinnedStringArray._

  private lazy val (start, end): (String, String) = bounds
  private lazy val normalizedLength = start.length
  private lazy val prefixLength = start.zip(end).indexWhere { case (l, r) => l != r }
  private lazy val prefix = start.substring(0, prefixLength)

  override protected def convertToLong(value: String): Long = {
    val normalized = normalize(value).padTo(normalizedLength, '0')
    if (normalized < start) { 0L } else if (normalized > end) { Long.MaxValue } else {
      // note: 12 is the most base-36 numbers we can fit in Long.MaxValue
      val sigDigits = normalized.substring(prefixLength).padTo(12, Base36Lowest).substring(0, 12)
      jLong.parseLong(sigDigits, 36)
    }
  }

  override protected def convertFromLong(value: Long): String =
    prefix + jLong.toString(value, 36).reverse.padTo(12, Base36Lowest).reverse.replaceFirst("0+$", "")
}

object BinnedStringArray {

  private val Base36Chars = (0 until 36).map(Integer.toString(_, 36).toLowerCase(Locale.US).charAt(0)).toArray
  val Base36Lowest: Char  = Base36Chars.head
  val Base36Highest: Char = Base36Chars.last

  def normalize(s: String): String = s.toLowerCase(Locale.US).replaceAll("[^0-9a-z]", Base36Lowest.toString)

  def normalizeBounds(bounds: (String, String)): (String, String) = {
    val length = math.max(bounds._1.length, bounds._2.length)
    val loBase36 = normalize(bounds._1)
    val hiBase36 = normalize(bounds._2)
    // pad them and make sure they are in sorted order
    val (loPadded, hiPadded) = if (loBase36 < hiBase36) {
      (loBase36.padTo(length, Base36Lowest), hiBase36.padTo(length, Base36Highest))
    } else {
      (hiBase36.padTo(length, Base36Lowest), loBase36.padTo(length, Base36Highest))
    }
    // ensure that they haven't become the same string
    val (loDistinct, hiDistinct) = if (loPadded == hiPadded) {
      (loPadded + Base36Lowest, hiPadded + Base36Highest)
    } else {
      (loPadded, hiPadded)
    }
    // check to make sure they fit in a long (12 chars)
    val prefixLength = loDistinct.zip(hiDistinct).indexWhere { case (l, r) => l != r }
    val loFit = if (loDistinct.length > prefixLength + 12) loDistinct.take(prefixLength + 12) else loDistinct
    val hiFit = if (hiDistinct.length > prefixLength + 12) hiDistinct.take(prefixLength + 12) else hiDistinct
    // check one last time that they aren't the same
    if (loFit == hiFit) {
      (loFit.dropRight(1) + Base36Lowest, hiFit.dropRight(1) + Base36Highest)
    } else {
      (loFit, hiFit)
    }
  }
}