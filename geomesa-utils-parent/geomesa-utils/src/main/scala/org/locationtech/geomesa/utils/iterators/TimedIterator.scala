/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator

/**
 * Class for gathering timing metrics on an iterator
 *
 * @param delegate iterator to time
 * @param callback timer callback, will be passed nanoseconds spent running the iterator
 * @tparam T type binding of iterator
 */
class TimedIterator[T](delegate: CloseableIterator[T], callback: Long => Any) extends CloseableIterator[T] {

  private var total = 0L

  override def hasNext: Boolean = {
    val start = System.nanoTime()
    try { delegate.hasNext } finally {
      total += (System.nanoTime() - start)
    }
  }

  override def next(): T = {
    val start = System.nanoTime()
    try { delegate.next() } finally {
      total += (System.nanoTime() - start)
    }
  }

  override def close(): Unit = {
    val start = System.nanoTime()
    try { delegate.close() } finally {
      total += (System.nanoTime() - start)
      callback(total)
    }
  }
}
