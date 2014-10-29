/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils

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

