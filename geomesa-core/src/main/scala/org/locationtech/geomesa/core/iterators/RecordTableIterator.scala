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

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.tables.AttributeTable._
import org.locationtech.geomesa.core.index._
import org.opengis.feature.`type`.AttributeDescriptor

import scala.util.{Failure, Success}

/**
 * Iterator for the record table. Applies transforms and ECQL filters.
 */
class RecordTableIterator
    extends HasIteratorExtensions
    with SortedKeyValueIterator[Key, Value]
    with HasFeatureType
    with HasFeatureDecoder
    with HasEcqlFilter
    with HasTransforms
    with Logging {

  var source: SortedKeyValueIterator[Key, Value] = null

  var topKey: Option[Key] = None
  var topValue: Option[Value] = None

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {

    TServerClassLoader.initClassLoader(logger)

    initFeatureType(options)
    init(featureType, options)

    this.source = source.deepCopy(env)
  }

  override def hasTop = topKey.isDefined

  override def getTopKey = topKey.orNull

  override def getTopValue = topValue.orNull

  /**
   * Seeks to the start of a range and fetches the top key/value
   *
   * @param range
   * @param columnFamilies
   * @param inclusive
   */
  override def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    // move the source iterator to the right starting spot
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  /**
   * Reads the next qualifying key/value
   */
  override def next() = findTop()

  /**
   * Advances the index-iterator to the next qualifying entry
   */
  def findTop() {

    // clear out the reference to the last entry
    topKey = None
    topValue = None

    // loop while there is more data and we haven't matched our filter
    while (topValue.isEmpty && source.hasTop) {

      val sourceValue = source.getTopValue
      // the value contains the full-resolution geometry and time
      lazy val feature = featureDecoder.decode(sourceValue.get())
      // TODO we could decode it already transformed if we check that the filters are covered
      // evaluate the filter check
      val meetsFilters = ecqlFilter.forall(fn => fn(feature))

      if (meetsFilters) {
        // current entry matches our filter - update the key and value
        // copy the key because reusing it is UNSAFE
        topKey = Some(new Key(source.getTopKey))
        // return the value or transform it as required
        topValue = transform.map(fn => new Value(fn(feature)))
            .orElse(Some(new Value(sourceValue)))
      }

      // increment the underlying iterator
      source.next()
    }
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("RecordTableIterator does not support deepCopy.")
}


