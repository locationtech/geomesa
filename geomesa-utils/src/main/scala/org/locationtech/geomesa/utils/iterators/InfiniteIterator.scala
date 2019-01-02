/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

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
