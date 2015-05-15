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

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

/**
 * Iterator for the record table. Applies transforms and ECQL filters.
 */
class RecordTableIterator
    extends GeomesaFilteringIterator
    with HasFeatureType
    with SetTopInclude
    with SetTopFilter
    with SetTopTransform
    with SetTopFilterTransform {

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)

    // pick the execution path once based on the filters and transforms we need to apply
    // see org.locationtech.geomesa.core.iterators.IteratorFunctions
    setTopOptimized = (filter, transform) match {
      case (null, null) => setTopInclude
      case (_, null)    => setTopFilter
      case (null, _)    => setTopTransform
      case (_, _)       => setTopFilterTransform
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)
}


