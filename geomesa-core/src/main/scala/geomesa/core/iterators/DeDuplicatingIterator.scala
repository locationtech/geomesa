/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.iterators

import DeDuplicator._
import java.util.Map.Entry
import java.util.UUID
import java.util.{Iterator=>JIterator}
import net.sf.ehcache.{Element, CacheManager}
import org.apache.accumulo.core.data.{Key, Value}

class KVEntry(akey:Key, avalue:Value) extends Entry[Key,Value] {
  def this(entry:Entry[Key,Value]) = this(entry.getKey, entry.getValue)

  var key:Key = if (akey==null) null else new Key(akey)
  var value:Value = if (avalue==null) null else new Value(avalue)

  def getValue : Value = value
  def getKey : Key = key
  def setValue(value:Value) : Value = { throw new UnsupportedOperationException }
  def setKey(key:Key) : Key = { throw new UnsupportedOperationException }
}

/**
 * Simple utility that removes duplicates from the list of IDs passed through.
 *
 * @param source the original iterator that may contain duplicate ID-rows
 * @param idFetcher the way to extract an ID from any one of the keys
 */
class DeDuplicatingIterator(source:JIterator[Entry[Key,Value]],
                            idFetcher:(Key,Value)=>String)
  extends Iterator[Entry[Key,Value]] {

  val deduper = new DeDuplicator(idFetcher)
  private[this] def findTop : Entry[Key,Value] = {
    var top : Entry[Key,Value] = null
    while (top==null && source.hasNext) {
      top = source.next
      if (deduper.isDuplicate(top))
        top = null
    }
    if (top==null) deduper.close
    top
  }
  var nextEntry : Entry[Key,Value] = findTop
  override def next : Entry[Key,Value] = { val result = nextEntry; nextEntry = findTop; result }
  override def hasNext : Boolean = nextEntry!=null
}

class DeDuplicator(idFetcher:(Key,Value)=>String) {
  val cacheName = UUID.randomUUID().toString

  cacheManager.addCache(cacheName)
  val cache = cacheManager.getCache(cacheName)

  val dummyConstant: Int = 0

  def isUnique(key:Key, value:Value) : Boolean = {
    val id : String = idFetcher(key, value)
    val result = !cache.isKeyInCache(id)
    cache.put(new Element(id, dummyConstant))
    result
  }

  def isDuplicate(key:Key, value:Value) : Boolean = !isUnique(key, value)

  def isDuplicate(entry:Entry[Key,Value]) : Boolean = {
    if (entry==null || entry.getKey==null) true
    else {
      !isUnique(entry.getKey, entry.getValue)
    }
  }

  // safe to call repeatedly
  def close() {
    if (cacheManager.cacheExists(cacheName))
      cacheManager.removeCache(cacheName)
  }
}

object DeDuplicator {
  val cacheManager = CacheManager.create()
}