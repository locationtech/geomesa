/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.util.control.NonFatal

/**
 * Delegates an iterator and throws all exceptions through the 'next' method to get around geotools
 * wrapping iterators that catch and suppress exceptions in hasNext
 *
 * @param delegate delegate iterator
 * @tparam T type bounds
 */
class ExceptionalIterator[T](delegate: Iterator[T]) extends Iterator[T] {

  private var _suppressed: Throwable = _

  override def hasNext: Boolean = {
    try { delegate.hasNext } catch {
      case NonFatal(e) =>
        _suppressed = e
        true
    }
  }

  override def next(): T = {
    if (_suppressed != null) {
      throw _suppressed
    } else {
      delegate.next()
    }
  }

  def suppressed: Option[Throwable] = Option(_suppressed)
}

object ExceptionalIterator {

  def apply[T](iterator: Iterator[T]): ExceptionalIterator[T] = new ExceptionalIterator(iterator)

  def apply[T](iterator: CloseableIterator[T]): ExceptionalCloseableIterator[T] =
    new ExceptionalCloseableIterator(iterator)

  class ExceptionalCloseableIterator[T](delegate: CloseableIterator[T])
      extends ExceptionalIterator(delegate) with CloseableIterator[T] {
    override def close(): Unit = delegate.close()
  }
}