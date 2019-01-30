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
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.locationtech.geomesa.utils.collection.CloseableIterator.CloseableIteratorImpl
import org.opengis.feature.Feature
import org.opengis.feature.`type`.FeatureType
import org.opengis.feature.simple.SimpleFeature

import scala.collection.Iterator

// By 'self-closing', we mean that the iterator will automatically call close once it is completely exhausted.
trait SelfClosingIterator[+A] extends CloseableIterator[A] {
  abstract override def hasNext: Boolean = {
    val res = super.hasNext
    if (!res) {
      close()
    }
    res
  }
}

object SelfClosingIterator {

  def apply[A](iter: Iterator[A], closeIter: => Unit = {}): SelfClosingIterator[A] =
    new CloseableIteratorImpl(iter, closeIter) with SelfClosingIterator[A]

  def apply[A](iter: Iterator[A] with Closeable): SelfClosingIterator[A] = apply(iter, iter.close())

  def apply[A](iter: CloseableIterator[A]): SelfClosingIterator[A] = apply(iter, iter.close())

  def apply[A <: Feature, B <: FeatureType](fr: FeatureReader[B, A]): SelfClosingIterator[A] =
    apply(CloseableIterator(fr))

  def apply(iter: SimpleFeatureIterator): SelfClosingIterator[SimpleFeature] = apply(CloseableIterator(iter))

  def apply(c: SimpleFeatureCollection): SelfClosingIterator[SimpleFeature] = apply(c.features)
}
