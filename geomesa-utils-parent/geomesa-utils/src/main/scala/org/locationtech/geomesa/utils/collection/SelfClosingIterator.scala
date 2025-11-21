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
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}

import java.io.Closeable

// By 'self-closing', we mean that the iterator will automatically call close once it is completely exhausted.
class SelfClosingIterator[+A](iter: Iterator[A], closeIter: => Unit) extends CloseableIterator[A] {
  override def hasNext: Boolean = {
    val res = iter.hasNext
    if (!res) {
      close()
    }
    res
  }
  override def next(): A = iter.next()
  override def close(): Unit = closeIter
}

object SelfClosingIterator {

  def apply[A](iter: Iterator[A], closeIter: => Unit = {}): SelfClosingIterator[A] = new SelfClosingIterator(iter, closeIter)

  def apply[A](iter: Iterator[A] with Closeable): SelfClosingIterator[A] = apply(iter, iter.close())

  def apply[A](iter: CloseableIterator[A]): SelfClosingIterator[A] = apply(iter, iter.close())

  def apply[A <: Feature, B <: FeatureType](fr: FeatureReader[B, A]): SelfClosingIterator[A] =
    apply(CloseableIterator(fr))

  def apply(iter: SimpleFeatureIterator): SelfClosingIterator[SimpleFeature] = apply(CloseableIterator(iter))

  def apply(c: SimpleFeatureCollection): SelfClosingIterator[SimpleFeature] = apply(c.features)
}
