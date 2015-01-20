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

/**
 * Iterator for the record table. Applies transforms and ECQL filters.
 */
class RecordTableIterator
    extends GeomesaFilteringIterator
    with HasFeatureType
    with HasFeatureDecoder
    with HasEcqlFilter
    with HasTransforms
    with Logging {

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)
  }

  override def setTopConditionally() {

    val sourceValue = source.getTopValue
    // the value contains the full-resolution geometry and time
    lazy val feature = featureDecoder.decode(sourceValue.get())
    // TODO we could decode it already transformed if we check that the filters are covered
    // evaluate the filter check
    val meetsFilters = ecqlFilter.forall(fn => fn(feature))

    if (meetsFilters) {
      // current entry matches our filter - update the key and value
      topKey = Some(source.getTopKey)
      // return the value or transform it as required
      topValue = transform.map(fn => new Value(fn(feature)))
          .orElse(Some(sourceValue))
    }
  }
}


