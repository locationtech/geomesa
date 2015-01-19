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
import org.locationtech.geomesa.utils.stats.MethodProfiling

/**
 * This is an Index Only Iterator, to be used in situations where the data records are
 * not useful enough to pay the penalty of decoding when using the
 * SpatioTemporalIntersectingIterator.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 */
class IndexIterator
    extends GeomesaFilteringIterator
    with HasFeatureBuilder
    with HasIndexValueDecoder
    with HasSpatioTemporalFilter
    with HasFeatureDecoder
    with HasTransforms
    with HasInMemoryDeduplication
    with MethodProfiling
    with Logging {

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)
  }

  override def setTopConditionally() = {

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
  }
}
