/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
 * @param seqs the list-of-lists whose items are to be recombined
 * @tparam T the type of items
 */
case class CartesianProductIterable[T](seqs:Seq[Seq[T]]) extends Iterable[Seq[T]] {
  // how many (total) combinations there will be in an iterator
  lazy val expectedSize : Long = seqs.map(seq => seq.size.toLong).product

  // create an iterator that will visit all possible combinations
  // (every time you call this, you get a NEW iterator)
  def iterator : Iterator[Seq[T]] = new Iterator[Seq[T]] {
    // variable index that counts off the combinations
    var index : Long = 0L
    // current value (combination)
    private def value : Seq[T] =
      seqs.foldLeft(Seq[T](),index)((t,src) => t match { case (seqSoFar,idx) => {
        val modulus : Int = src.size
        val itemIndex : Int = (idx % modulus).toInt
        val item : T = src(itemIndex)
        (seqSoFar++Seq(item), idx / modulus)
      }})._1
    def next() : Seq[T] = {
      val result = if (hasNext) value else null
      index = index + 1L
      result
    }
    def hasNext : Boolean = index < expectedSize
  }
}

