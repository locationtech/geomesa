/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import java.io.Closeable

import org.geotools.data.FeatureReader
import org.geotools.feature.FeatureIterator
import org.locationtech.geomesa.utils.collection.CloseableIterator.{CloseableIteratorImpl, ConcatCloseableIterator, FlatMapCloseableIterator}
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.Feature
import org.opengis.feature.`type`.FeatureType
import org.opengis.feature.simple.SimpleFeature

import scala.annotation.tailrec
import scala.collection.{GenTraversableOnce, Iterator}

// A CloseableIterator is one which involves some kind of close function which should be called at the end of use.
object CloseableIterator {

  private val empty: CloseableIterator[Nothing] = apply(Iterator.empty)

  // noinspection LanguageFeature
  // implicit promoting wrapper for convenience
  implicit def iteratorToCloseable[A](iter: Iterator[A]): CloseableIterator[A] = apply(iter)

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A](iter: Iterator[A], close: => Unit = Unit): CloseableIterator[A] =
    new CloseableIteratorImpl[A](iter, close)

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A <: Feature, B <: FeatureType](iter: FeatureReader[B, A]): CloseableIterator[A] =
    new CloseableFeatureReaderIterator(iter)

  def apply(iter: FeatureIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
    new CloseableFeatureIterator(iter)

  def single[A](elem: => A, close: => Unit = Unit): CloseableIterator[A] =
    new CloseableSingleIterator(elem, close)

  def fill[A](length: Int, close: => Unit = Unit)(elem: => A): CloseableIterator[A] =
    new CloseableIteratorImpl(Iterator.fill(length)(elem), close)

  def empty[A]: CloseableIterator[A] = empty

  private def wrap[A](t: GenTraversableOnce[A]): CloseableIterator[A] = t match {
    case c: CloseableIterator[A] => c
    case c => new CloseableIteratorImpl(c.toIterator, Unit)
  }

  class CloseableIteratorImpl[A](iter: Iterator[A], closeIter: => Unit) extends CloseableIterator[A] {
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

    override def ++[B >: A](that: => GenTraversableOnce[B]): CloseableIterator[B] = {
      lazy val applied = CloseableIterator.wrap(that)
      new ConcatCloseableIterator[B](queue.+:(() => current).:+(() => applied))
    }
  }

  private final class FlatMapCloseableIterator[A, B](source: CloseableIterator[A], f: A => GenTraversableOnce[B])
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

trait CloseableIterator[+A] extends Iterator[A] with Closeable {

  override def map[B](f: A => B): CloseableIterator[B] = new CloseableIteratorImpl(super.map(f), close())

  override def filter(p: A => Boolean): CloseableIterator[A] = new CloseableIteratorImpl(super.filter(p), close())

  override def take(n: Int): CloseableIterator[A] = new CloseableIteratorImpl(super.take(n), close())

  override def collect[B](pf: PartialFunction[A, B]): CloseableIterator[B] =
    new CloseableIteratorImpl(super.collect(pf), close())

  override def ++[B >: A](that: => GenTraversableOnce[B]): CloseableIterator[B] = {
    lazy val applied = CloseableIterator.wrap(that)
    val queue = new scala.collection.mutable.Queue[() => CloseableIterator[B]]
    queue.enqueue(() => this)
    queue.enqueue(() => applied)
    new ConcatCloseableIterator[B](queue)
  }

  override def flatMap[B](f: A => GenTraversableOnce[B]): CloseableIterator[B] =
    new FlatMapCloseableIterator(this, f)
}
