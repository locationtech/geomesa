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
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.core.index
import org.locationtech.geomesa.utils.stats.{AutoLoggingTimings, MethodProfiling, NoOpTimings, Timings}

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
    extends HasIteratorExtensions
    with SortedKeyValueIterator[Key, Value]
    with HasFeatureType
    with HasFeatureDecoder
    with HasSpatioTemporalFilter
    with HasEcqlFilter
    with HasTransforms
    with HasInMemoryDeduplication
    with MethodProfiling
    with Logging {

  var topKey: Option[Key] = None
  var topValue: Option[Value] = None
  var source: SortedKeyValueIterator[Key, Value] = null
  var dtgIndex: Option[Int] = None

  override def init(
      source: SortedKeyValueIterator[Key, Value],
      options: java.util.Map[String, String],
      env: IteratorEnvironment) = {

    TServerClassLoader.initClassLoader(logger)
    initFeatureType(options)
    init(featureType, options)
    dtgIndex = index.getDtgFieldName(featureType).map(featureType.indexOf(_))

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
   * Advances the index-iterator to the next qualifying entry
   */
  def findTop() {

    // clear out the reference to the last entry
    topKey = None
    topValue = None

    // loop while there is more data and we haven't matched our filter
    while (topValue.isEmpty && source.hasTop) {

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

      // increment the underlying iterator
      source.next()
    }
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("SpatioTemporalIntersectingIterator does not support deepCopy")
}

object SpatioTemporalIntersectingIterator {
  implicit val timings: Timings = new AutoLoggingTimings()
  implicit val noOpTimings: Timings = new NoOpTimings()
}