/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

/**
  * Does a sorted merge of already sorted streams
  *
  * @param streams streams, each individually sorted
  * @param ordering ordering
  * @tparam T type bounds
  */
class SortedMergeIterator[T](streams: Seq[CloseableIterator[T]])(implicit ordering: Ordering[T])
    extends CloseableIterator[T] {

  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

  private val indexedStreams = streams.toIndexedSeq
  // reverse the ordering so we get the head of the queue as the first value
  private val heads = scala.collection.mutable.PriorityQueue.empty[(T, Int)](Ordering.Tuple2(ordering.reverse, Ordering.Int))

  streams.foreachIndex { case (s, i) => if (s.hasNext) { heads += ((s.next, i)) } }

  override def hasNext: Boolean = heads.nonEmpty

  override def next(): T = {
    val (n, i) = heads.dequeue()
    val stream = indexedStreams(i)
    if (stream.hasNext) {
      heads += ((stream.next, i))
    }
    n
  }

  override def close(): Unit = streams.foreach(CloseWithLogging.apply)
}
