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

package org.locationtech.geomesa.core.iterators

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{ByteSequence, Range, Value, Key}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.core.index

abstract class GeomesaFilteringIterator
    extends HasIteratorExtensions with SortedKeyValueIterator[Key, Value] with Logging {

  var topKey: Option[Key] = None
  var topValue: Option[Value] = None
  var source: SortedKeyValueIterator[Key, Value] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    TServerClassLoader.initClassLoader(logger)
    this.source = source.deepCopy(env)
  }

  override def hasTop = topKey.isDefined

  override def getTopKey = topKey.orNull

  override def getTopValue = topValue.orNull

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
    topKey = None
    topValue = None

    // loop while there is more data and we haven't matched our filter
    while (source.hasTop && !checkTop()) {
      // increment the underlying iterator
      // it's important not to advance this if we're returning the current source top
      source.next()
    }
  }

  def checkTop(): Boolean = {
    setTopConditionally()
    topValue.isDefined
  }

  /**
   * Subclasses should set top key and top value if the current row is valid to be returned
   */
  def setTopConditionally(): Unit

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("GeoMesa iterators do not support deepCopy")
}
