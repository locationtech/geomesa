/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.utils

import scala.collection.Iterator.empty

object InfiniteIterator {

  implicit class InfiniteIterator[A](val iter: Iterator[A]) {

    /** @param p the predicate defining which element to stop after
      *
      * @return a [[Iterator]] which will delegate to the underlying iterator and return all elements up to
      *         and including the first element ``a`` for which p(a) == true after which no more calls will
      *         be made to the underlying iterator
      */
    def stopAfter(p: A => Boolean): Iterator[A] = new Iterator[A] {
      private var done = false

      override def hasNext = !done && iter.hasNext

      override def next() = {
        if (!done) {
          val next = iter.next()
          done = p(next)
          next
        } else {
          empty.next()
        }
      }
    }
  }
}
