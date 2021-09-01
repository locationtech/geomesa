/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
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
 * @param cache cache of feature ids seen so far
 * @param maxCacheSize max size of the feature id cache
 */
class DeduplicatingSimpleFeatureIterator(
    source: CloseableIterator[SimpleFeature],
    cache: scala.collection.mutable.Set[String] = scala.collection.mutable.HashSet.empty[String],
    maxCacheSize: Int = 999999
  ) extends CloseableIterator[SimpleFeature] {

  private val iter =
    source.filter(sf => if (cache.size < maxCacheSize) { cache.add(sf.getID) } else { !cache.contains(sf.getID) })

  override def hasNext: Boolean = iter.hasNext
  override def next(): SimpleFeature = iter.next()
  override def close(): Unit = source.close()
}
