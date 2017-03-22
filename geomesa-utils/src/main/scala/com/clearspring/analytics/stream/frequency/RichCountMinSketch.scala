/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.clearspring.analytics.stream.frequency

/**
  * Extends counts min sketch with public access to protected variables for more efficient operations.
  */
class RichCountMinSketch(val sketch: CountMinSketch) extends AnyVal {

  def width = sketch.width
  def depth = sketch.depth

  def table: Array[Array[Long]] = sketch.table
  def setSize(size: Long): Unit = sketch.size = size

  def add(other: CountMinSketch): Unit = {
    // note: we assume that seed is equal
    if (sketch.depth != other.depth || sketch.width != other.width) {
      throw new IllegalArgumentException("Can't merge CountMinSketch of different sizes")
    }
    var i = 0
    while (i < sketch.table.length) {
      val row = sketch.table(i)
      val otherRow = other.table(i)
      var j = 0
      while (j < row.length) {
        row(j) += otherRow(j)
        j += 1
      }
      i += 1
    }
    sketch.size += other.size
  }

  def clear(): Unit = {
    var i = 0
    while (i < sketch.depth) {
      val row = sketch.table(i)
      var j = 0
      while (j < sketch.width) {
        row(j) = 0L
        j += 1
      }
      i += 1
    }
    sketch.size = 0L
  }

  def isEquivalent(other: CountMinSketch): Boolean = {
    if (sketch.size != other.size || sketch.depth != other.depth || sketch.width != other.width) {
      return false
    }
    var i = 0
    while (i < sketch.depth) {
      if (sketch.hashA(i) != other.hashA(i)) {
        return false
      }
      val row = sketch.table(i)
      val otherRow = other.table(i)
      var j = 0
      while (j < sketch.width) {
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

