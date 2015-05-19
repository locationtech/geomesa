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

package org.locationtech.geomesa.raster.iterators

import java.util.{Map => JMap}

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.raster.index.RasterEntry

class RasterFilteringIterator
  extends GeomesaFilteringIterator
  with HasFeatureType
  with SetTopUnique
  with SetTopFilter
  with HasFilter {

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)
    logger.debug(s"In RFI with $filter")

    setTopOptimized = filter match {
      case null => setTopInclude
      case _ => setTopFilter
    }
  }

  override def setTopFilter(key: Key): Unit = {
    val value = source.getTopValue
    val sf = RasterEntry.decodeIndexCQMetadataToSf(key.getColumnQualifierData.toArray)
    if (filter.evaluate(sf)) {
      topKey = key
      topValue = value
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)

}
