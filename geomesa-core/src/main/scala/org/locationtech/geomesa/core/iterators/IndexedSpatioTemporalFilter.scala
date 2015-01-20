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
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

class IndexedSpatioTemporalFilter
    extends GeomesaFilteringIterator
    with HasFeatureType
    with HasIndexValueDecoder
    with HasSpatioTemporalFilter
    with Logging {

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)
    this.source = source.deepCopy(env)
  }

  override def setTopConditionally() = {
    val sourceValue = source.getTopValue
    val meetsFilter = stFilter.forall { fn =>
      val sf = indexEncoder.decode(sourceValue.get)
      fn(sf.geom, sf.date.map(_.getTime))
    }
    if (meetsFilter) {
      topKey = Some(source.getTopKey)
      topValue = Some(sourceValue)
    }
  }
}
