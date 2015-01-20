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

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.core.index
import org.locationtech.geomesa.utils.stats.MethodProfiling

/**
 * This iterator returns as its nextKey and nextValue responses the key and value
 * from the DATA iterator, not from the INDEX iterator.  The assumption is that
 * the data rows are what we care about; that we do not care about the index
 * rows that merely helped us find the data rows quickly.
 *
 * The other trick to remember about iterators is that they essentially pre-fetch
 * data.  "hasNext" really means, "was there a next record that you already found".
 */
class SpatioTemporalIntersectingIterator
    extends GeomesaFilteringIterator
    with HasFeatureType
    with HasFeatureDecoder
    with HasSpatioTemporalFilter
    with HasEcqlFilter
    with HasTransforms
    with HasInMemoryDeduplication
    with MethodProfiling
    with Logging {

  var dtgIndex: Option[Int] = None

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)
    dtgIndex = index.getDtgFieldName(featureType).map(featureType.indexOf(_))
  }

  override def setTopConditionally(): Unit = {
    val key = source.getTopKey
    if (!SpatioTemporalTable.isDataEntry(key)) {
      logger.warn("Found unexpected index entry: " + key)
    } else {
      if (checkUniqueId.forall(fn => fn(key.getColumnQualifier.toString))) {
        val dataValue = source.getTopValue
        lazy val sf = featureDecoder.decode(dataValue.get)
        val meetsStFilter = stFilter.forall(fn => fn(sf.getDefaultGeometry.asInstanceOf[Geometry],
          dtgIndex.flatMap(i => Option(sf.getAttribute(i).asInstanceOf[Date]).map(_.getTime))))
        val meetsFilters =  meetsStFilter && ecqlFilter.forall(fn => fn(sf))
        if (meetsFilters) {
          // update the key and value
          topKey = Some(key)
          // apply any transform here
          topValue = transform.map(fn => new Value(fn(sf))).orElse(Some(dataValue))
        }
      }
    }
  }
}
