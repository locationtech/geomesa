/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

abstract class GeomesaFilteringIterator
    extends HasIteratorExtensions with SortedKeyValueIterator[Key, Value] with HasSourceIterator with Logging {

  var source: SortedKeyValueIterator[Key, Value] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    TServerClassLoader.initClassLoader(logger)
    this.source = source.deepCopy(env)
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