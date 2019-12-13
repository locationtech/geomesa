/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.clearspring

import com.clearspring.analytics.stream.cardinality.RegisterSet.getSizeForCount

class RegisterSet(val count: Int)(private val M: Array[Int] = Array.ofDim(getSizeForCount(count))) {

  import com.clearspring.analytics.stream.cardinality.RegisterSet._

  val size: Int = M.length

  def set(position: Int, value: Int): Unit = {
    val bucketPos = position / LOG2_BITS_PER_WORD
    val shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD))
    M(bucketPos) = (M(bucketPos) & ~(0x1f << shift)) | (value << shift)
  }

  def get(position: Int): Int = {
    val bucketPos = position / LOG2_BITS_PER_WORD
    val shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD))
    (M(bucketPos) & (0x1f << shift)) >>> shift
  }

  def updateIfGreater(position: Int, value: Int): Boolean = {
    val bucket = position / LOG2_BITS_PER_WORD
    val shift = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD))
    val mask = 0x1f << shift

    // Use long to avoid sign issues with the left-most shift
    val curVal = this.M(bucket).toLong & mask
    val newVal = value.toLong << shift
    if (curVal < newVal) {
      this.M(bucket) = ((this.M(bucket) & ~mask) | newVal).toInt
      true
    } else {
      false
    }
  }

  def merge(that: RegisterSet): Unit = {
    var bucket = 0
    while (bucket < M.length) {
      var word = 0
      var j = 0
      while (j < LOG2_BITS_PER_WORD) {
        val mask = 0x1f << (REGISTER_SIZE * j)
        val thisVal = this.M(bucket) & mask
        val thatVal = that.M(bucket) & mask
        if (thisVal < thatVal) { word |= thatVal } else { word |= thisVal }
        j += 1
      }
      M(bucket) = word
      bucket += 1
    }
  }

  def rawBits: Array[Int] = M
}