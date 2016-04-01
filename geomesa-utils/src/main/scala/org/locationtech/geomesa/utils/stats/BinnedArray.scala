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

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.text.WKTUtils

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
    * Min bounds convenience method
    *
    * @return min bounds
    */
  def min: T = bounds._1

  /**
    * Max bounds convenience method
    *
    * @return max bounds
    */
  def max: T = bounds._2
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
    } else if (clas == classOf[Date]) {
      new BinnedDateArray(length, bounds.asInstanceOf[(Date, Date)])
    } else if (classOf[Geometry].isAssignableFrom(clas)) {
      new BinnedGeometryArray(length, bounds.asInstanceOf[(Geometry, Geometry)])
    } else {
      throw new UnsupportedOperationException(s"BinnedArray not implemented for ${clas.getName}")
    }
    ba.asInstanceOf[BinnedArray[T]]
  }
}

class BinnedIntegerArray(length: Int, bounds: (Integer, Integer)) extends BinnedArray[Integer](length, bounds) {

  require(bounds._1 < bounds._2,
    s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val binSize = (bounds._2 - bounds._1).toFloat / length

  override def indexOf(value: Integer): Int = {
    if (value < bounds._1 || value > bounds._2) { -1 } else {
      val i = math.floor((value - bounds._1) / binSize).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
    }
  }

  override def medianValue(index: Int): Integer = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val i = bounds._1 + math.round(binSize / 2 + binSize * index)
    if (i > bounds._2) bounds._2 else i
  }

  override def bounds(index: Int): (Integer, Integer) = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val lo = bounds._1 + math.ceil(binSize * index).toInt
    val hi = math.max(lo, bounds._1 + math.floor(binSize * (index + 1)).toInt)
    (if (lo > bounds._2) bounds._2 else lo, if (hi > bounds._2) bounds._2 else hi)
  }
}

class BinnedLongArray(length: Int, bounds: (jLong, jLong)) extends BinnedArray[jLong](length, bounds) {

  require(bounds._1 < bounds._2,
    s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val binSize = (bounds._2 - bounds._1).toDouble / length

  override def indexOf(value: jLong): Int = {
    if (value < bounds._1 || value > bounds._2) { -1 } else {
      val i = math.floor((value - bounds._1) / binSize).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
    }
  }

  override def medianValue(index: Int): jLong = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val i = bounds._1 + math.round(binSize / 2 + binSize * index)
    if (i > bounds._2) bounds._2 else i
  }

  override def bounds(index: Int): (jLong, jLong) = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val lo = bounds._1 + math.ceil(binSize * index).toLong
    val hi = math.max(lo, bounds._1 + math.floor(binSize * (index + 1)).toLong)
    (if (lo > bounds._2) bounds._2 else lo, if (hi > bounds._2) bounds._2 else hi)
  }
}


class BinnedFloatArray(length: Int, bounds: (jFloat, jFloat)) extends BinnedArray[jFloat](length, bounds) {

  require(bounds._1 < bounds._2,
    s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val binSize = (bounds._2 - bounds._1) / length

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
    s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val binSize = (bounds._2 - bounds._1) / length

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

  require(start < end, s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

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
  def longToString(l: Long, padding: Char = '0'): String =
    start.substring(0, prefixLength) + jLong.toString(l, 36).padTo(sigDigits, padding)

  override def indexOf(value: String): Int = {
    val normalized = normalize(value)
    if (normalized < start || normalized > end) { -1 } else {
      val i = math.floor((stringToLong(normalized) - minLong) / binSize).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
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

class BinnedDateArray(length: Int, bounds: (Date, Date)) extends BinnedArray[Date](length, bounds) {

  private val minLong = bounds._1.getTime
  private val maxLong = bounds._2.getTime
  private val binSize = math.max(1, (maxLong - minLong).toDouble / length)

  require(minLong < bounds._2.getTime,
    s"Upper bound must be after lower bound: lower=${bounds._1} upper=${bounds._2}")

  override def indexOf(value: Date): Int = {
    val time = value.getTime
    if (time < minLong || time > maxLong) { -1 } else {
      val i = math.floor((time - minLong) / binSize).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
    }
  }

  override def medianValue(index: Int): Date = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val i = minLong + math.round(binSize / 2 + binSize * index)
    new Date(if (i > maxLong) maxLong else i)
  }

  override def bounds(index: Int): (Date, Date) = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    val loLong = minLong + math.ceil(binSize * index).toLong
    val hiLong = minLong + math.floor(binSize * (index + 1)).toLong - 1
    (new Date(if (loLong > maxLong) maxLong else loLong), new Date(if (hiLong > maxLong) maxLong else hiLong))
  }
}

/**
  * Sorts geometries based on the geohash of their centroid, as an int
  *
  * @param length number of bins
  * @param bounds upper and lower bounds for the input values
  */
class BinnedGeometryArray(length: Int, bounds: (Geometry, Geometry)) extends BinnedArray[Geometry](length, bounds) {

  private val minInt = geomToInt(bounds._1)
  private val maxInt = geomToInt(bounds._2)

  require(minInt < maxInt,
    s"GeoHashes aren't ordered: lower=${WKTUtils.write(bounds._1)}:$minInt upper=${WKTUtils.write(bounds._2)}:$maxInt")

  private val binSize = math.max(1, (maxInt - minInt).toFloat / length)

  private def intToGeoHash(i: Int): GeoHash = {
    val asBinaryString = Integer.toBinaryString(i)
    val asPaddedString = f"$asBinaryString%10s".replaceAll(" ", "0")
    GeoHash.fromBinaryString(asPaddedString)
  }

  /**
    * Gets a geohash as an int value
    *
    * @param value geometry to evaluate (will use the centroid)
    * @param length length of geohash, in 5-bit digits (base 32)
    * @return
    */
  private def geomToInt(value: Geometry, length: Int = 2): Int = {
    val centroid = value.getCentroid
    GeoHash(centroid.getX, centroid.getY, 5 * length).toInt
  }


  override def indexOf(value: Geometry): Int = {
    val gh = geomToInt(value)
    if (gh < minInt || gh > maxInt) { -1 } else {
      val i = math.floor((gh - minInt) / binSize).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > length) -1 else if (i == length) length - 1 else i
    }
  }

  override def medianValue(index: Int): Geometry = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    // the way geohashes work, we want to use floor instead of round otherwise we can jump around
    intToGeoHash(minInt + math.floor(binSize / 2 + binSize * index).toInt).getPoint
  }

  override def bounds(index: Int): (Geometry, Geometry) = {
    if (index < 0 || index > length) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    // the way geohashes work, we want to use floor instead of round otherwise we can jump around
    val bbox = intToGeoHash(minInt + math.floor(binSize / 2 + binSize * index).toInt).bbox
    (bbox.ll, bbox.ur)
  }
}
