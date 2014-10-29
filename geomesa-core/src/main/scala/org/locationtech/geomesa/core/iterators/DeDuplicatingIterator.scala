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

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.accumulo.core.data.{Key, Value}
import org.locationtech.geomesa.core.util.CloseableIterator

class KVEntry(akey: Key, avalue: Value) extends Entry[Key, Value] {
  def this(entry: Entry[Key, Value]) = this(entry.getKey, entry.getValue)

  var key   = if (akey == null) null else new Key(akey)
  var value = if (avalue == null) null else new Value(avalue)

  def getValue = value
  def getKey   = key

  def setValue(value: Value) = ???
  def setKey(key: Key)       = ???
}

/**
 * Simple utility that removes duplicates from the list of IDs passed through.
 *
 * @param source the original iterator that may contain duplicate ID-rows
 * @param idFetcher the way to extract an ID from any one of the keys
 */
class DeDuplicatingIterator(source: CloseableIterator[Entry[Key, Value]],
                            idFetcher:(Key, Value) => String)
  extends CloseableIterator[Entry[Key, Value]] {

  val deduper = new DeDuplicator(idFetcher)
  private[this] def findTop = {
    var top: Entry[Key,Value] = null
    while (top == null && source.hasNext) {
      top = source.next
      if (deduper.isDuplicate(top)) top = null
    }
    if (top == null) deduper.close
    top
  }
  var nextEntry = findTop
  override def next : Entry[Key, Value] = {
    val result = nextEntry
    nextEntry = findTop; result
  }

  override def hasNext = nextEntry != null

  override def close(): Unit = source.close()
}

class DeDuplicator(idFetcher: (Key, Value) => String) {
  val cache: Cache[String, Integer] = CacheBuilder.newBuilder().build()

  val dummyConstant = 0

  def isUnique(key:Key, value:Value): Boolean = {
    val id = idFetcher(key, value)
    val entry = cache.getIfPresent(id)
    if (entry == null) {
      cache.put(id, dummyConstant)
      true
    }
    else false
  }

  def isDuplicate(key: Key, value: Value): Boolean = !isUnique(key, value)

  def isDuplicate(entry: Entry[Key, Value]): Boolean =
    if (entry == null || entry.getKey == null) true
    else !isUnique(entry.getKey, entry.getValue)

  def close() {
    cache.invalidateAll()
  }
}