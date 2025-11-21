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
 * Class for gathering timing metrics on an iterator
 *
 * @param delegate iterator to time
 * @param callback timer callback, will be passed nanoseconds spent running the iterator
 * @tparam T type binding of iterator
 */
class TimedIterator[T](delegate: CloseableIterator[T], callback: Long => Unit) extends CloseableIterator[T] {

  private var total = 0L

  private val callbacks = ArrayBuffer(callback)

  /**
   * Add another callback
   *
   * @param callback callback to be executed when iterator is closed, will be passed nanoseconds spent running the iterator
   * @return this iterator, for method chaining
   */
  def addCallback(callback: Long => Unit): TimedIterator[T] = {
    callbacks += callback
    this
  }

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
      callbacks.foreach(_.apply(total))
    }
  }
}

object TimedIterator {
  def empty[T](): TimedIterator[T] = new TimedIterator(CloseableIterator.empty, _ => ())
}
