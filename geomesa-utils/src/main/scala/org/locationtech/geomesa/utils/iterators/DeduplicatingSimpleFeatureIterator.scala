/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature

/**
 * Simple utility that removes duplicates from the list of IDs passed through.
 *
 * @param source the original iterator that may contain duplicate ID-rows
 */
class DeduplicatingSimpleFeatureIterator(source: CloseableIterator[SimpleFeature], maxCacheSize: Int = 999999)
    extends CloseableIterator[SimpleFeature] {

  private val cache = scala.collection.mutable.HashSet.empty[String]
  private var nextEntry = findNext()

  override def next(): SimpleFeature = {
    val next = nextEntry
    nextEntry = findNext()
    next
  }

  override def hasNext: Boolean = nextEntry != null

  private def findNext(): SimpleFeature = {
    var next: SimpleFeature = null
    do {
      next = if (source.hasNext) source.next() else null
    } while (next != null && (if (cache.size < maxCacheSize) !cache.add(next.getID) else cache.contains(next.getID)))
    next
  }

  override def close(): Unit = {
    cache.clear()
    source.close()
  }
}
