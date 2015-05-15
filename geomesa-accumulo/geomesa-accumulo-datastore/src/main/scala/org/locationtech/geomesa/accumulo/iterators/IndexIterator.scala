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

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.data.tables.SpatioTemporalTable
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
    with HasFeatureType
    with SetTopIndexUnique
    with SetTopIndexFilterUnique
    with SetTopIndexTransformUnique
    with SetTopIndexFilterTransformUnique {

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)

    // pick the execution path once based on the filters and transforms we need to apply
    // see org.locationtech.geomesa.core.iterators.IteratorFunctions
    setTopOptimized = (stFilter, transform, checkUniqueId) match {
      case (null, null, null) => setTopIndexInclude
      case (null, null, _)    => setTopIndexUnique
      case (_, null, null)    => setTopIndexFilter
      case (_, null, _)       => setTopIndexFilterUnique
      case (null, _, null)    => setTopIndexTransform
      case (null, _, _)       => setTopIndexTransformUnique
      case (_, _, null)       => setTopIndexFilterTransform
      case (_, _, _)          => setTopIndexFilterTransformUnique
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)
}
