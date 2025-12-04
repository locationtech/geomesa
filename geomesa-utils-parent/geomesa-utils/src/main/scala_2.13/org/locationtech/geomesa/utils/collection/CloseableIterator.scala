/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import org.geotools.api.data.FeatureReader
import org.geotools.api.feature.Feature
import org.geotools.api.feature.`type`.FeatureType
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.feature.FeatureIterator
import org.locationtech.geomesa.utils.collection.CloseableIterator.{CloseableIteratorImpl, ConcatCloseableIterator, FlatMapCloseableIterator}
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.io.Closeable
import scala.annotation.tailrec

/**
 * A CloseableIterator is an iterator that should be closed after use
 */
object CloseableIterator {

  private val empty: CloseableIterator[Nothing] = apply(Iterator.empty)

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A](iter: Iterator[A]): CloseableIterator[A] =
    new CloseableIteratorImpl[A](iter, Option(iter).collect { case c: Closeable => c.close() })

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A](iter: Iterator[A], close: => Unit): CloseableIterator[A] = new CloseableIteratorImpl[A](iter, close)

  // for wrapping java iterators
  def apply[A](iter: java.util.Iterator[A]): CloseableIterator[A] =
    new CloseableIteratorJavaWrapper[A](iter, Option(iter).collect { case c: Closeable => c.close() })

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A <: Feature, B <: FeatureType](iter: FeatureReader[B, A]): CloseableIterator[A] =
    new CloseableFeatureReaderIterator(iter)

  def apply(iter: FeatureIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = new CloseableFeatureIterator(iter)

  def single[A](elem: A, close: => Unit = ()): CloseableIterator[A] = new CloseableSingleIterator(elem, close)

  def fill[A](length: Int, close: => Unit = ())(elem: => A): CloseableIterator[A] =
    new CloseableIteratorImpl(Iterator.fill(length)(elem), close)

  def empty[A]: CloseableIterator[A] = empty

  def wrap[A](t: IterableOnce[A]): CloseableIterator[A] = t match {
    case c: CloseableIterator[A] => c
    case c: Closeable => CloseableIterator(t.iterator, c.close())
    case c => new CloseableIteratorImpl(c.iterator, ())
  }

  private class CloseableIteratorImpl[A](iter: Iterator[A], closeIter: => Unit) extends CloseableIterator[A] {
    override def hasNext: Boolean = iter.hasNext
    override def next(): A = iter.next()
    override def close(): Unit = closeIter
  }

  private class CloseableIteratorJavaWrapper[A](iter: java.util.Iterator[A], closeIter: => Unit) extends CloseableIterator[A] {
    override def hasNext: Boolean = iter.hasNext
    override def next(): A = iter.next()
    override def close(): Unit = closeIter
  }

  private final class CloseableFeatureReaderIterator[A <: Feature, B <: FeatureType](iter: FeatureReader[B, A])
      extends CloseableIterator[A] {
    override def hasNext: Boolean = iter.hasNext
    override def next(): A = iter.next()
    override def close(): Unit = iter.close()
  }

  private final class CloseableFeatureIterator(iter: FeatureIterator[SimpleFeature])
      extends CloseableIterator[SimpleFeature] {
    override def hasNext: Boolean = iter.hasNext
    override def next(): SimpleFeature  = iter.next()
    override def close(): Unit = iter.close()
  }

  private final class CloseableSingleIterator[A](elem: => A, closeIter: => Unit) extends CloseableIterator[A] {
    private var result = true
    override def hasNext: Boolean = result
    override def next(): A = if (result) { result = false; elem } else { empty.next() }
    override def close(): Unit = closeIter
  }

  /**
    * Based on scala's ++ iterator implementation
    *
    * Avoid stack overflows when applying ++ to lots of iterators by flattening the unevaluated
    * iterators out into a vector of closures.
    */
  private final class ConcatCloseableIterator[+A](queue: scala.collection.mutable.Queue[() => CloseableIterator[A]])
      extends CloseableIterator[A] {

    private [this] var current: CloseableIterator[A] = queue.dequeue()()

    // Advance current to the next non-empty iterator
    // current is set to empty when all iterators are exhausted
    @tailrec
    private [this] def advance(): Boolean = {
      current.close()
      if (queue.isEmpty) {
        current = CloseableIterator.empty
        false
      } else {
        current = queue.dequeue()()
        current.hasNext || advance()
      }
    }

    override def hasNext: Boolean = current.hasNext || advance()
    override def next(): A = current.next

    override def close(): Unit = {
      current.close()
      queue.foreach(_.apply().close())
      queue.clear()
    }

    override def concat[B >: A](xs: => IterableOnce[B]): CloseableIterator[B] = {
      lazy val applied = CloseableIterator.wrap(xs)
      new ConcatCloseableIterator[B](queue.+:(() => current).:+(() => applied))
    }
  }

  private final class FlatMapCloseableIterator[A, B](source: CloseableIterator[A], f: A => IterableOnce[B])
      extends CloseableIterator[B] {

    private var cur: CloseableIterator[B] = empty

    @tailrec
    override def hasNext: Boolean = cur.hasNext || {
      cur.close()
      if (source.hasNext) {
        cur = CloseableIterator.wrap(f(source.next()))
        hasNext
      } else {
        cur = empty
        false
      }
    }

    override def next(): B = if (hasNext) { cur.next() } else { empty.next() }

    override def close(): Unit = CloseQuietly(cur, source).foreach(f => throw f)
  }
}

/**
 * An iterator that should be closed after use.
 *
 * For convenience, some methods that consume the iterator will automatically close the iterator when invoked:
 *
 * <ul>
 * <li>`toList`</li>
 * <li>`foreach`</li>
 * <li>`size`</li>
 * </ul>
 *
 * Note that other methods (for example, `length`) may end up invoking these methods, due to the internal implementation of
 * collections.
 *
 * @tparam A iterator values
 */
trait CloseableIterator[+A] extends Iterator[A] with Closeable {

  // note: in scala 2.13, toList *does not* call foreach, so we need to close the iterator here
  override def toList: List[A] = try { super.toList } finally { close() }

  override def foreach[U](f: A => U): Unit = try { super.foreach(f) } finally { close() }

  override def size: Int =  try { super.size } finally { close() }

  override def map[B](f: A => B): CloseableIterator[B] = new CloseableIteratorImpl(super.map(f), close())

  override def filter(p: A => Boolean): CloseableIterator[A] = new CloseableIteratorImpl(super.filter(p), close())

  override def withFilter(p: A => Boolean): CloseableIterator[A] = filter(p)

  override def filterNot(p: A => Boolean): CloseableIterator[A] = new CloseableIteratorImpl(super.filterNot(p), close())

  override def take(n: Int): CloseableIterator[A] = new CloseableIteratorImpl(super.take(n), close())

  override def takeWhile(p: A => Boolean): CloseableIterator[A] = new CloseableIteratorImpl(super.takeWhile(p), close())

  override def drop(n: Int): CloseableIterator[A] = new CloseableIteratorImpl(super.drop(n), close())

  override def dropWhile(p: A => Boolean): CloseableIterator[A] = new CloseableIteratorImpl(super.dropWhile(p), close())

  override def collect[B](pf: PartialFunction[A, B]): CloseableIterator[B] = new CloseableIteratorImpl(super.collect(pf), close())

  override def concat[B >: A](xs: => IterableOnce[B]): CloseableIterator[B] = {
    lazy val applied = CloseableIterator.wrap(xs)
    val queue = new scala.collection.mutable.Queue[() => CloseableIterator[B]]
    queue.enqueue(() => this)
    queue.enqueue(() => applied)
    new ConcatCloseableIterator[B](queue)
  }

  override def flatMap[B](f: A => IterableOnce[B]): CloseableIterator[B] = new FlatMapCloseableIterator(this, f)

  override def tapEach[U](f: A => U): CloseableIterator[A] = new CloseableIteratorImpl(super.tapEach(f), close())
}
