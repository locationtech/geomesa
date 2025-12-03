/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.mutable.ArrayBuffer

/**
 * Class for gathering a count for an iterator
 *
 * @param delegate iterator to count
 * @param counter counting function
 * @param callback count callback
 * @tparam T type binding of iterator
 */
class CountingIterator[T](delegate: CloseableIterator[T], counter: T => Int, callback: Long => Unit)
    extends CloseableIterator[T] {

  private var total = 0L

  private val callbacks = ArrayBuffer(callback)

  /**
   * Add another callback
   *
   * @param callback callback to be executed when iterator is closed, will be passed nanoseconds spent running the iterator
   * @return this iterator, for method chaining
   */
  def addCallback(callback: Long => Unit): CountingIterator[T] = {
    callbacks += callback
    this
  }

  override def hasNext: Boolean = delegate.hasNext

  override def next(): T = {
    val result = delegate.next()
    total += counter(result)
    result
  }

  override def close(): Unit = {
    try { delegate.close() } finally {
      callbacks.foreach(_.apply(total))
    }
  }
}
