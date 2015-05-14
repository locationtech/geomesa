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

/**
 * Simple utility that removes duplicates from the list of IDs passed through.
 *
 * @param source the original iterator that may contain duplicate ID-rows
 * @param idFetcher the way to extract an ID from any one of the keys
 */
class DeDuplicatingIterator(source: CloseableIterator[Entry[Key, Value]], idFetcher:(Key, Value) => String)
  extends CloseableIterator[Entry[Key, Value]] {

  val deduper = new DeDuplicator(idFetcher)

  var nextEntry = findTop

  private[this] def findTop = {
    var top: Entry[Key,Value] = null
    while (top == null && source.hasNext) {
      top = source.next
      if (deduper.isDuplicate(top)) {
        top = null
      }
    }
    top
  }

  override def next : Entry[Key, Value] = {
    val result = nextEntry
    nextEntry = findTop
    result
  }

  override def hasNext = nextEntry != null

  override def close(): Unit = {
    deduper.close
    source.close()
  }
}

class DeDuplicator(idFetcher: (Key, Value) => String) {

  val cache = scala.collection.mutable.HashSet.empty[String]

  def isUnique(key: Key, value: Value): Boolean = cache.add(idFetcher(key, value))

  def isDuplicate(key: Key, value: Value): Boolean = !isUnique(key, value)

  def isDuplicate(entry: Entry[Key, Value]): Boolean =
    if (entry == null || entry.getKey == null) {
      true
    } else {
      !isUnique(entry.getKey, entry.getValue)
    }

  def close() = cache.clear()
}