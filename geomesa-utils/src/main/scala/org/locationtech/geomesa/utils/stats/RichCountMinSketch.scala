/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import com.clearspring.analytics.stream.frequency.CountMinSketch
import RichCountMinSketch._
/**
  * Extends counts min sketch with public access to protected variables for more efficient operations.
  */
class RichCountMinSketch(val sketch: CountMinSketch) extends AnyVal {

  def width = reflectWidth(sketch)
  def depth = reflectDepth(sketch)

  def table: Array[Array[Long]] = reflectTable(sketch)
  def setSize(size: Long): Unit = reflectSetSize(sketch, size)

  def add(other: CountMinSketch): Unit = {
    // note: we assume that seed is equal
    if (depth != reflectDepth(other) || width != reflectWidth(other)) {
      throw new IllegalArgumentException("Can't merge CountMinSketch of different sizes")
    }
    val t = table
    var i = 0
    while (i < t.length) {
      val row = t(i)
      val otherRow = reflectTable(other)(i)
      var j = 0
      while (j < row.length) {
        row(j) += otherRow(j)
        j += 1
      }
      i += 1
    }
    setSize(sketch.size + other.size)
  }

  def clear(): Unit = {
    var i = 0
    while (i < depth) {
      val row = table(i)
      var j = 0
      while (j < width) {
        row(j) = 0L
        j += 1
      }
      i += 1
    }
    setSize(0L)
  }

  def isEquivalent(other: CountMinSketch): Boolean = {
    if (sketch.size != other.size || depth != reflectDepth(other) || width != reflectWidth(other)) {
      return false
    }
    var i = 0
    while (i < depth) {
      if (reflectHashA(sketch)(i) != reflectHashA(other)(i)) {
        return false
      }
      val t = table
      val row = table(i)
      val otherRow = reflectTable(other)(i)
      var j = 0
      while (j < width) {
        if (row(j) != otherRow(j)) {
          return false
        }
        j += 1
      }
      i += 1
    }
    true
  }
}

object RichCountMinSketch {

  private val widthField = classOf[CountMinSketch].getDeclaredField("width")
  widthField.setAccessible(true)

  private val depthField = classOf[CountMinSketch].getDeclaredField("depth")
  depthField.setAccessible(true)

  private val tableField = classOf[CountMinSketch].getDeclaredField("table")
  tableField.setAccessible(true)

  private val sizeField = classOf[CountMinSketch].getDeclaredField("size")
  sizeField.setAccessible(true)

  private val hashAField = classOf[CountMinSketch].getDeclaredField("hashA")
  hashAField.setAccessible(true)

  def reflectWidth(cms: CountMinSketch) = widthField.getInt(cms)
  def reflectDepth(cms: CountMinSketch) = depthField.getInt(cms)
  def reflectTable(cms: CountMinSketch) = tableField.get(cms).asInstanceOf[Array[Array[Long]]]
  def reflectSize(cms: CountMinSketch)  = sizeField.getLong(cms)
  def reflectHashA(cms: CountMinSketch) = hashAField.get(cms).asInstanceOf[Array[Long]]

  def reflectSetSize(cms: CountMinSketch, v: Long) = sizeField.setLong(cms, v)

}
