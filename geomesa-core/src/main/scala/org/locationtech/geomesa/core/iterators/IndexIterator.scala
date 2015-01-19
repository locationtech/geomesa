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

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.utils.stats.{AutoLoggingTimings, MethodProfiling, NoOpTimings, Timings}

/**
 * This is an Index Only Iterator, to be used in situations where the data records are
 * not useful enough to pay the penalty of decoding when using the
 * SpatioTemporalIntersectingIterator.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 */
class IndexIterator
    extends HasIteratorExtensions
    with SortedKeyValueIterator[Key, Value]
    with HasFeatureBuilder
    with HasIndexValueDecoder
    with HasSpatioTemporalFilter
    with HasFeatureDecoder
    with HasTransforms
    with HasInMemoryDeduplication
    with MethodProfiling
    with Logging {

  protected var topKey: Option[Key] = None
  protected var topValue: Option[Value] = None
  protected var source: SortedKeyValueIterator[Key, Value] = null

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

      val indexKey = source.getTopKey

      if (!SpatioTemporalTable.isIndexEntry(indexKey)) {
        // if this is a data entry, skip it
        logger.warn("Found unexpected data entry: " + indexKey)
      } else {
        // the value contains the full-resolution geometry and time plus feature ID
        val decodedValue = indexEncoder.decode(source.getTopValue.get)

        // evaluate the filter checks, in least to most expensive order
        val meetsIndexFilters = checkUniqueId.forall(fn => fn(decodedValue.id)) &&
            stFilter.forall(fn => fn(decodedValue.geom, decodedValue.date.map(_.getTime)))

        if (meetsIndexFilters) { // we hit a valid geometry, date and id
          val transformedFeature = encodeIndexValueToSF(decodedValue)
          // update the key and value
          // copy the key because reusing it is UNSAFE
          topKey = Some(indexKey)
          topValue = transform.map(fn => new Value(fn(transformedFeature)))
              .orElse(Some(new Value(featureEncoder.encode(transformedFeature))))
        }
      }

      // increment the underlying iterator
      source.next()
    }
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("IndexIterator does not support deepCopy.")
}

object IndexIterator {
  implicit val timings: Timings = new AutoLoggingTimings()
  implicit val noOpTimings: Timings = new NoOpTimings()
}
