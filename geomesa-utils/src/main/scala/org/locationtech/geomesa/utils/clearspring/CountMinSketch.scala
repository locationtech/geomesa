/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/**
  * Adapted from:
  *
  * stream-lib
  * Copyright 2016 AddThis
  *
  * This product includes software developed by AddThis.
  *
  * This product also includes code adapted from:
  *
  * Apache Solr (http://lucene.apache.org/solr/)
  * Copyright 2014 The Apache Software Foundation
  *
  * Apache Mahout (http://mahout.apache.org/)
  * Copyright 2014 The Apache Software Foundation
  *
  */

package org.locationtech.geomesa.utils.clearspring

import com.clearspring.analytics.stream.frequency.IFrequency
import com.clearspring.analytics.stream.membership.Filter

/**
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
class CountMinSketch private (val eps: Double,
                              val confidence: Double,
                              private [utils] val table: Array[Array[Long]],
                              private [utils] var _size: Long,
                              private val depth: Int,
                              private val width: Int,
                              private val hashA: Array[Long]) extends IFrequency {

  def getRelativeError: Double = eps
  def getConfidence: Double = confidence

  private def hash(item: Long, i: Int): Int = {
    var hash = hashA(i) * item
    // A super fast way of computing x mod 2^p-1
    // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
    // page 149, right after Proposition 7.
    hash += hash >> 32
    hash &= CountMinSketch.PrimeModulus
    // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
    hash.toInt % width
  }

  override def add(item: Long, count: Long): Unit = {
    if (count < 0) {
      // Actually for negative increments we'll need to use the median
      // instead of minimum, and accuracy will suffer somewhat.
      // Probably makes sense to add an "allow negative increments"
      // parameter to constructor.
      throw new IllegalArgumentException("Negative increments not implemented")
    }
    var i = 0
    while (i < depth) {
      table(i)(hash(item, i)) += count
      i += 1
    }
    _size += count
  }

  override def add(item: String, count: Long): Unit = {
    if (count < 0) {
      // Actually for negative increments we'll need to use the median
      // instead of minimum, and accuracy will suffer somewhat.
      // Probably makes sense to add an "allow negative increments"
      // parameter to constructor.
      throw new IllegalArgumentException("Negative increments not implemented")
    }
    val buckets = Filter.getHashBuckets(item, depth, width)
    var i = 0
    while (i < depth) {
      table(i)(buckets(i)) += count
      i += 1
    }
    _size += count
  }

  /**
    * The estimate is correct within 'epsilon' * (total item count),
    * with probability 'confidence'.
    */
  override def estimateCount(item: Long): Long = {
    var res = Long.MaxValue
    var i = 0
    while (i < depth) {
      val row = table(i)
      res = math.min(res, row(hash(item, i)))
      i += 1
    }
    res
  }

  override def estimateCount(item: String): Long = {
    var res = Long.MaxValue
    val buckets = Filter.getHashBuckets(item, depth, width)
    var i = 0
    while (i < depth) {
      res = Math.min(res, table(i)(buckets(i)))
      i += 1
    }
    res
  }

  def +=(other: CountMinSketch): Unit = {
    // note: we assume that seed is equal
    if (depth != other.depth || width != other.width) {
      throw new IllegalArgumentException("Can't merge CountMinSketch of different sizes")
    }
    var i, j = 0
    while (i < table.length) {
      val row = table(i)
      val otherRow = other.table(i)
      while (j < row.length) {
        row(j) += otherRow(j)
        j += 1
      }
      i += 1
      j = 0
    }
    _size += other.size
  }

  def clear(): Unit = {
    var i, j = 0
    while (i < depth) {
      val row = table(i)
      while (j < width) {
        row(j) = 0L
        j += 1
      }
      i += 1
      j = 0
    }
    _size = 0L
  }

  def isEquivalent(other: CountMinSketch): Boolean = {
    if (size != other.size || depth != other.depth || width != other.width) {
      return false
    }
    var i, j = 0
    while (i < depth) {
      if (hashA(i) != other.hashA(i)) {
        return false
      }
      val row = table(i)
      val otherRow = other.table(i)
      while (j < width) {
        if (row(j) != otherRow(j)) {
          return false
        }
        j += 1
      }
      i += 1
      j = 0
    }
    true
  }

  override def size: Long = _size
}

object CountMinSketch {

  private val PrimeModulus: Long = (1L << 31) - 1

  def apply(eps: Double, confidence: Double, seed: Int): CountMinSketch = {

    // 2/w = eps ; w = 2/eps
    // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)

    val width = math.ceil(2 / eps).toInt
    val depth = math.ceil(-1 * math.log(1 - confidence) / math.log(2)).toInt

    val table: Array[Array[Long]] = Array.fill(depth)(Array.fill(width)(0L))

    // We're using a linear hash functions
    // of the form (a*x+b) mod p.
    // a,b are chosen independently for each hash function.
    // However we can set b = 0 as all it does is shift the results
    // without compromising their uniformity or independence with
    // the other hashes.
    val hashA: Array[Long] = {
      val r = new java.util.Random(seed)
      Array.fill(depth)(r.nextInt(Int.MaxValue))
    }

    new CountMinSketch(eps, confidence, table, 0L, depth, width, hashA)
  }
}
