/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.iterators

import collection.JavaConverters._
import com.vividsolutions.jts.geom._
import geomesa.core.index.{SpatioTemporalIndexEntry, SpatioTemporalIndexSchema}
import geomesa.utils.geohash.GeoHash
import geomesa.utils.text.WKTUtils
import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import java.util.{HashSet => JHashSet}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.geotools.data.DataUtilities
import org.geotools.factory.GeoTools
import org.joda.time.{DateTimeZone, DateTime, Interval}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.util.Try
import geomesa.core.data._
import scala.Some

/**
 * This is an Index Only Iterator, to be used in situations where the data records are
 * not useful enough to pay the penalty of decoding which one must pay when using the
 * SpatioTemporalIntersectingIterator.
 *
 * This iterator returns as its nextKey the key for the INDEX iterator. nextValue is
 * always null.
 *
 * Note that this extends the SpatioTemporalIntersectingIterator, but never creates a dataSource
 * and hence never iterates through it.
 */
class IndexIterator extends SpatioTemporalIntersectingIterator with SortedKeyValueIterator[Key, Value] {

  import SpatioTemporalIndexEntry._
  import geomesa.core._

  override def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    log.debug("Initializing classLoader")
    IndexIterator.initClassLoader(log)

    val featureType = DataUtilities.createType("DummyType", options.get(DEFAULT_FEATURE_TYPE))

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    val featureEncoder = SimpleFeatureEncoderFactory.createEncoder(encodingOpt)

    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
    schema = SpatioTemporalIndexSchema(schemaEncoding, featureType, featureEncoder)
    if (options.containsKey(DEFAULT_POLY_PROPERTY_NAME)) {
      val polyWKT = options.get(DEFAULT_POLY_PROPERTY_NAME)
      poly = WKTUtils.read(polyWKT)
    }
    if (options.containsKey(DEFAULT_INTERVAL_PROPERTY_NAME))
      interval = IndexIterator.decodeInterval(
        options.get(DEFAULT_INTERVAL_PROPERTY_NAME))
    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
    deduplicate = SpatioTemporalIndexSchema.mayContainDuplicates(featureType)

    this.indexSource = source.deepCopy(env)
  }


  /**
   * Advances the index-iterator to the next qualifying entry
   */
  override def findTop() {
    // clear out the reference to the next entry
    nextKey = null

    // be sure to start on an index entry
    skipDataEntries(indexSource)

    while (indexSource.hasTop && indexSource.getTopKey != null) {
      // only consider this index entry if we could fully decode the key
      decodeKey(indexSource.getTopKey).map { decodedKey =>
        curFeature = decodedKey
        // the value contains the full-resolution geometry and time; use them
        lazy val decodedValue = SpatioTemporalIndexSchema.decodeIndexValue(indexSource.getTopValue)
        lazy val isGeomAcceptable: Boolean = wrappedGeomFilter(decodedKey.gh, decodedValue.geom)
        lazy val isDateTimeAcceptable: Boolean = wrappedTimeFilter(decodedValue.dtgMillis)

        // see whether this box is acceptable
        // (the tests are ordered from fastest to slowest to take advantage of
        // short-circuit evaluation)
        if (isIdUnique(decodedValue.id) && isDateTimeAcceptable && isGeomAcceptable) {
          // stash this ID
          rememberId(decodedValue.id)
        }
      }
      // you MUST advance to the next key
      indexSource.next()
       // skip over any intervening data entries, should they exist
      skipDataEntries(indexSource)
    }
  }
  override def deepCopy(env: IteratorEnvironment) = throw new UnsupportedOperationException("IndexIterator does not support deepCopy.")
}

object IndexIterator extends IteratorHelperObject
