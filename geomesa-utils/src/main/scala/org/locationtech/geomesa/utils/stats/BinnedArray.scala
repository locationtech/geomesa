/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.{Date, Locale}

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
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
    val lo = min + math.ceil(binSize * index).toLong
    val hi = math.max(lo, min + math.floor(binSize * (index + 1)).toLong)
    (if (lo > max) bounds._2 else convertFromLong(lo), if (hi > max) bounds._2 else convertFromLong(hi, hi = true))
  }

  /**
    * Maps a value to a long used to allocate values in bins
    *
    * @param value value to convert
    * @param hi true if this is an upper value, false if it's a lower or middle value
    * @return value as a long
    */
  protected def convertToLong(value: T, hi: Boolean = false): Long

  /**
    * Maps a long back to a value
    *
    * @param value value as a long
    * @param hi true if this is an upper value, false if it's a lower or middle value
    * @return value
    */
  protected def convertFromLong(value: Long, hi: Boolean = false): T
}

class BinnedIntegerArray(length: Int, bounds: (Integer, Integer)) extends WholeNumberBinnedArray[Integer](length, bounds) {
  override protected def convertToLong(value: Integer, hi: Boolean): Long = value.toLong
  override protected def convertFromLong(value: Long, hi: Boolean): Integer = value.toInt
}

class BinnedLongArray(length: Int, bounds: (jLong, jLong)) extends WholeNumberBinnedArray[jLong](length, bounds) {
  override protected def convertToLong(value: jLong, hi: Boolean): Long = value
  override protected def convertFromLong(value: Long, hi: Boolean): jLong = value
}

class BinnedDateArray(length: Int, bounds: (Date, Date)) extends WholeNumberBinnedArray[Date](length, bounds) {
  override protected def convertToLong(value: Date, hi: Boolean): Long = value.getTime
  override protected def convertFromLong(value: Long, hi: Boolean): Date = new Date(if (hi) value - 1 else value)
}

/**
  * Sorts geometries based on the z-value of their centroid
  *
  * @param length number of bins
  * @param bounds upper and lower bounds for the input values
  */
class BinnedGeometryArray(length: Int, bounds: (Geometry, Geometry))
    extends WholeNumberBinnedArray[Geometry](length, bounds) {

  override protected def convertToLong(value: Geometry, hi: Boolean): Long = {
    val centroid = value match {
      case p: Point => p
      case g => g.getCentroid
    }
    Z2SFC.index(centroid.getX, centroid.getY).z
  }

  override protected def convertFromLong(value: Long, hi: Boolean): Geometry = {
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
}

/**
  * Bins strings. Will set up bins based on the longest-common-prefix of the bounds. Estimates
  * bins by considering inputs to be roughly equivalent to base36 longs.
  *
  * @param length number of bins
  * @param bounds upper and lower bounds for the input values
  */
class BinnedStringArray(length: Int, bounds: (String, String)) extends BinnedArray[String](length, bounds) {

  private val (start, end) = {
    val lower = normalize(bounds._1)
    val upper = normalize(bounds._2)
    if (lower.length == upper.length) {
      (lower, upper)
    } else if (lower.length < upper.length) {
      (lower.padTo(upper.length, '0'), upper)
    } else {
      (lower, upper.padTo(lower.length, 'z'))
    }
  }

  require(start < end,
    s"Upper bound must be greater than lower bound: lower='${bounds._1}'($start) upper='${bounds._2}'($end)")

  // # of base 36 numbers we can consider
  private val sigDigits = math.max(1, math.round(math.log(length) / math.log(36)).toInt)
  private val prefixLength = start.zip(end).indexWhere { case (l, r) => l != r }

  private val minLong = stringToLong(start)
  private val maxLong = stringToLong(end, 'z')

  private val binSize = (maxLong - minLong).toDouble / length

  private def normalize(s: String) = s.toLowerCase(Locale.US).replaceAll("[^0-9a-z]", "0")

  def stringToLong(s: String, padding: Char = '0'): Long = {
    val normalized = if (s.length >= prefixLength + sigDigits) {
      s.substring(prefixLength, prefixLength + sigDigits)
    } else {
      s.substring(prefixLength).padTo(sigDigits, padding)
    }
    jLong.parseLong(normalized, 36)
  }
  def longToString(long: Long, padding: Char = '0'): String =
    start.substring(0, prefixLength) + jLong.toString(long, 36).padTo(sigDigits, padding)

  override def directIndex(value: Long): Int = {
    val i = math.floor((value - minLong) / binSize).toInt
    // i == length check catches the upper bound
    if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
  }

  override def indexOf(value: String): Int = {
    val normalized = normalize(value)
    if (normalized < start || normalized > end) { -1 } else {
      directIndex(stringToLong(normalized))
    }
  }

  override def medianValue(index: Int): String = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val i = minLong + math.round(binSize / 2 + binSize * index)
    longToString(if (i > maxLong) maxLong else i)
  }

  override def bounds(index: Int): (String, String) = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val loLong = minLong + math.ceil(binSize * index).toLong
    val hiLong = math.max(loLong, minLong + math.floor(binSize * (index + 1)).toLong)
    val lo = longToString(if (loLong > maxLong) maxLong else loLong)
    val hi = longToString(if (hiLong > maxLong) maxLong else hiLong, 'z')
    (lo, hi)
  }
}