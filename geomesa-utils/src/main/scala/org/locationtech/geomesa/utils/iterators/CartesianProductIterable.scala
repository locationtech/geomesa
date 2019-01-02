/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

/**
 * Can create an iterator over all combinations of items from a list-of-lists.
 * Because the final list of combinations can be large, we allow for a safe
 * way to query the list size that is independent of the iterator itself.
 * (That is, asking for the size does not exhaust any iterator.)
 *
 * NB:  The first sequence is the least significant; that is, it will
 * increment fast while the last sequence is the most significant (will
 * increment slowly).
 *
 * @param seqs the list-of-lists whose items are to be recombined
 * @tparam T the type of items
 */

case class CartesianProductIterable(seqs: Seq[Seq[_]]) extends Iterable[Seq[_]] {
  lazy val expectedSize: Long = seqs.map(_.size.toLong).product

  def iterator: Iterator[Seq[_]] = new Iterator[Seq[_]] {
    val n: Int = seqs.size
    val maxes: Vector[Int] = seqs.map(seq => seq.size).toVector
    val indexes = new scala.collection.mutable.ArraySeq[Int](seqs.size)
    var nextItem: Seq[_] = if (isValid) realize else null

    def isValid: Boolean = (0 until n).forall(i => indexes(i) < maxes(i))

    def realize: Seq[_] = (0 until n).map(i => seqs(i)(indexes(i)))

    def hasNext: Boolean = nextItem != null

    def next(): Seq[_] = {
      if (nextItem == null) throw new Exception("Iterator exhausted")
      val result = nextItem

      // advance the internal state
      nextItem = null
      var j = 0
      var done = false
      while (j < n && !done) {
        indexes(j) = indexes(j) + 1
        if (indexes(j) >= maxes(j)) {
          indexes(j) = 0
          j = j + 1
        } else {
          done = true
        }
      }
      if (done || j < n) nextItem = realize

      result
    }
  }
}

