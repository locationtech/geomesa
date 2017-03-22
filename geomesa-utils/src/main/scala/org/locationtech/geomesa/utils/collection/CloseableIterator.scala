/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import java.io.Closeable

import org.geotools.data.FeatureReader
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.opengis.feature.Feature
import org.opengis.feature.`type`.FeatureType
import org.opengis.feature.simple.SimpleFeature

import scala.annotation.tailrec
import scala.collection.Iterator

// A CloseableIterator is one which involves some kind of close function which should be called at the end of use.
object CloseableIterator {

  // In order to use 'map' and 'flatMap', we provide an implicit promoting wrapper.
  // noinspection LanguageFeature
  implicit def iteratorToCloseable[A](iter: Iterator[A]): CloseableIterator[A] = apply(iter)

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A](iter: Iterator[A], closeIter: => Unit = {}): CloseableIterator[A] =
    new CloseableIterator[A] {
      def hasNext = iter.hasNext
      def next()  = iter.next()
      def close() = closeIter
    }

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A <: Feature, B <: FeatureType](iter: FeatureReader[B, A]): CloseableIterator[A] =
    new CloseableIterator[A] {
      def hasNext = iter.hasNext
      def next()  = iter.next()
      def close() = iter.close()
    }

  def apply(iter: SimpleFeatureIterator): CloseableIterator[SimpleFeature] =
    new CloseableIterator[SimpleFeature] {
      def hasNext = iter.hasNext
      def next()  = iter.next()
      def close() = iter.close()
    }

  val empty: CloseableIterator[Nothing] = apply(Iterator.empty)
}

trait CloseableIterator[+A] extends Iterator[A] with Closeable {

  self =>

  import CloseableIterator.empty

  override def map[B](f: A => B): CloseableIterator[B] = CloseableIterator(super.map(f), self.close())

  override def filter(p: A => Boolean): CloseableIterator[A] = CloseableIterator(super.filter(p), self.close())

  // NB: Since we wish to be able to close the iterator currently in use, we can't call out to super.flatMap.
  def ciFlatMap[B](f: A => CloseableIterator[B]): CloseableIterator[B] = new SelfClosingIterator[B] {
    private var cur: CloseableIterator[B] = if(self.hasNext) f(self.next()) else empty

    // Add in the 'SelfClosing' behavior.
    def hasNext: Boolean = {
      @tailrec
      def loopUntilHasNext: Boolean =
        cur.hasNext || self.hasNext && {
          if (!cur.eq(empty)) {
            cur.close()
          }
          cur = f(self.next())
          loopUntilHasNext
        }

      val iterHasNext = loopUntilHasNext
      if (!iterHasNext) {
        close()
      }
      iterHasNext
    }

    def next(): B = (if (hasNext) cur else empty).next()

    def close() = { cur.close(); self.close() }
  }
}

// By 'self-closing', we mean that the iterator will automatically call close once it is completely exhausted.
trait SelfClosingIterator[+A] extends CloseableIterator[A]

object SelfClosingIterator {

  def apply[A](iter: Iterator[A], closeIter: () => Unit): SelfClosingIterator[A] =
    new SelfClosingIterator[A] {
      def hasNext: Boolean = {
        val iterHasNext = iter.hasNext
        if (!iterHasNext) {
          close()
        }
        iterHasNext
      }
      def next(): A = iter.next()
      def close() = closeIter()
    }

  def apply[A](iter: Iterator[A] with Closeable): SelfClosingIterator[A] = apply(iter, iter.close)

  def apply[A](iter: CloseableIterator[A]): SelfClosingIterator[A] = apply(iter, iter.close)

  def apply[A <: Feature, B <: FeatureType](fr: FeatureReader[B, A]): SelfClosingIterator[A] =
    apply(CloseableIterator(fr))

  def apply(iter: SimpleFeatureIterator): SelfClosingIterator[SimpleFeature] = apply(CloseableIterator(iter))

  def apply(c: SimpleFeatureCollection): SelfClosingIterator[SimpleFeature] = apply(c.features)
}
