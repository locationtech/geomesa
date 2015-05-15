/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.iterators

import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Value}
import org.locationtech.geomesa.core.util.CloseableIterator
import org.opengis.feature.simple.SimpleFeature

/**
 * Simple utility that removes duplicates from the list of IDs passed through.
 *
 * @param source the original iterator that may contain duplicate ID-rows
 */
class DeDuplicatingIterator(source: CloseableIterator[SimpleFeature], maxCacheSize: Int = 999999)
    extends CloseableIterator[SimpleFeature] {

  private var nextEntry: SimpleFeature = null
  private val cache = scala.collection.mutable.HashSet.empty[String]

  override def next(): SimpleFeature = nextEntry

  override def hasNext: Boolean = {
    while (setNext() && (cache.size < maxCacheSize && !cache.add(nextEntry.getID))) {}
    nextEntry != null
  }

  private def setNext(): Boolean = {
    if (source.hasNext) {
      nextEntry = source.next()
      true
    } else {
      nextEntry = null
      false
    }
  }

  override def close(): Unit = {
    cache.clear()
    source.close()
  }
}
