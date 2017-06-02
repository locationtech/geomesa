/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import java.io.Closeable

import org.geotools.data.FeatureReader
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.geotools.feature.FeatureIterator
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
  def apply[A](iter: Iterator[A], closeIter: => Unit = {}): CloseableIterator[A] =
    new CloseableIterator[A] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): A = iter.next()
      override def close(): Unit = closeIter
    }

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A <: Feature, B <: FeatureType](iter: FeatureReader[B, A]): CloseableIterator[A] =
    new CloseableIterator[A] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): A = iter.next()
      override def close(): Unit = iter.close()
    }

  def apply(iter: FeatureIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
    new CloseableIterator[SimpleFeature] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): SimpleFeature  = iter.next()
      override def close(): Unit = iter.close()
    }

  def empty[A]: CloseableIterator[A] = empty

  private def wrap[A](t: GenTraversableOnce[A]): CloseableIterator[A] = t match {
    case c: CloseableIterator[A] => c
    case c => CloseableIterator(c.toIterator)
  }
}

trait CloseableIterator[+A] extends Iterator[A] with Closeable {

  self =>

  import CloseableIterator.empty

  override def map[B](f: A => B): CloseableIterator[B] = CloseableIterator(super.map(f), self.close())

  override def filter(p: A => Boolean): CloseableIterator[A] = CloseableIterator(super.filter(p), self.close())

  override def ++[B >: A](that: => GenTraversableOnce[B]): CloseableIterator[B] = {
    lazy val applied = CloseableIterator.wrap(that)
    val queue = new scala.collection.mutable.Queue[() => CloseableIterator[B]]
    queue.enqueue(() => self)
    queue.enqueue(() => applied)
    new ConcatCloseableIterator[B](queue)
  }

  override def flatMap[B](f: A => GenTraversableOnce[B]): CloseableIterator[B] = new CloseableIterator[B] {
    private var cur: CloseableIterator[B] = empty

    @tailrec
    override def hasNext: Boolean = cur.hasNext || {
      cur.close()
      if (self.hasNext) {
        cur = CloseableIterator.wrap(f(self.next()))
        hasNext
      } else {
        cur = empty
        false
      }
    }

    override def next(): B = (if (hasNext) cur else empty).next()

    override def close(): Unit = { cur.close(); self.close() }
  }
}

/**
  * Based on scala's ++ iterator implementation
  *
  * Avoid stack overflows when applying ++ to lots of iterators by flattening the unevaluated
  * iterators out into a vector of closures.
  */
private [collection] final class ConcatCloseableIterator[+A](queue: scala.collection.mutable.Queue[() => CloseableIterator[A]])
    extends CloseableIterator[A] {

  private [this] var current: CloseableIterator[A] = queue.dequeue()()

  // Advance current to the next non-empty iterator
  // current is set to empty when all iterators are exhausted
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
    lazy val applied = that match {
      case c: CloseableIterator[B] => c
      case c => CloseableIterator(c.toIterator)
    }
    new ConcatCloseableIterator(queue.+:(() => current).:+(() => applied))
  }
}

// By 'self-closing', we mean that the iterator will automatically call close once it is completely exhausted.
trait SelfClosingIterator[+A] extends CloseableIterator[A]

object SelfClosingIterator {

  def apply[A](iter: Iterator[A], closeIter: => Unit = {}): SelfClosingIterator[A] =
    new SelfClosingIterator[A] {
      override def hasNext: Boolean = {
        val iterHasNext = iter.hasNext
        if (!iterHasNext) {
          close()
        }
        iterHasNext
      }
      override def next(): A = iter.next()
      override def close(): Unit = closeIter
    }

  def apply[A](iter: Iterator[A] with Closeable): SelfClosingIterator[A] = apply(iter, iter.close())

  def apply[A](iter: CloseableIterator[A]): SelfClosingIterator[A] = apply(iter, iter.close())

  def apply[A <: Feature, B <: FeatureType](fr: FeatureReader[B, A]): SelfClosingIterator[A] =
    apply(CloseableIterator(fr))

  def apply(iter: SimpleFeatureIterator): SelfClosingIterator[SimpleFeature] = apply(CloseableIterator(iter))

  def apply(c: SimpleFeatureCollection): SelfClosingIterator[SimpleFeature] = apply(c.features)
}
