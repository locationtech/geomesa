/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

import java.util.{Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

class IndexedSpatioTemporalFilter
    extends HasIteratorExtensions
    with SortedKeyValueIterator[Key, Value]
    with HasFeatureType
    with HasSpatioTemporalFilter
    with Logging {

  var topKey: Option[Key] = None
  var topValue: Option[Value] = None
  var source: SortedKeyValueIterator[Key, Value] = null

  override def init(
      source: SortedKeyValueIterator[Key, Value],
      options: JMap[String, String],
      env: IteratorEnvironment) {
    initFeatureType(options)
    init(featureType, options)
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

  override def next() = findTop()

  /**
   * Iterates over the source until an acceptable key/value pair is found.
   */
  private def findTop() = {
    // clear out the reference to the last entry
    topKey = None
    topValue = None

    // loop while there is more data and we haven't matched our filter
    while (topValue.isEmpty && source.hasTop) {
      val sourceValue = source.getTopValue
      val meetsFilter = stFilter.forall { fn =>
        val sf = indexEncoder.decode(sourceValue.get)
        fn(sf.geom, sf.date.map(_.getTime))
      }
      if (meetsFilter) {
        topKey = Some(source.getTopKey)
        topValue = Some(sourceValue)
      }
      source.next()
    }
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("IndexedSpatioTemporalFilter does not support deepCopy")
}
