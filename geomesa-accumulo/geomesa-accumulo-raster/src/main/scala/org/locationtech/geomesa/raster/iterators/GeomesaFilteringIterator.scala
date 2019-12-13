/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

abstract class GeomesaFilteringIterator
    extends HasIteratorExtensions with SortedKeyValueIterator[Key, Value] with HasSourceIterator with LazyLogging {

  var source: SortedKeyValueIterator[Key, Value] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    this.source = source
  }

  override def hasTop = topKey != null

  override def getTopKey = topKey

  override def getTopValue = topValue

  override def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    // move the source iterator to the right starting spot
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def next() = {
    source.next()
    findTop()
  }

  /**
   * Advances the index-iterator to the next qualifying entry
   */
  def findTop() = {
    // clear out the reference to the last entry
    topKey = null
    topValue = null

    // loop while there is more data and we haven't matched our filter
    while (source.hasTop && !checkTop()) {
      // increment the underlying iterator
      // it's important not to advance this if we're returning the current source top
      source.next()
    }
  }

  def checkTop(): Boolean = {
    setTopConditionally()
    topValue != null
  }

  /**
   * Subclasses should set top key and top value if the current row is valid to be returned
   */
  def setTopConditionally(): Unit

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new UnsupportedOperationException("GeoMesa iterators do not support deepCopy")
}

trait HasSourceIterator {
  def source: SortedKeyValueIterator[Key, Value]
  var topKey: Key = null
  var topValue: Value = null
}
