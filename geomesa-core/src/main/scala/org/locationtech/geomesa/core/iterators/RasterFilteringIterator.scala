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

import java.util.{Date, HashSet => JHashSet}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import scala.util.Try


class RasterFilteringIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  protected var rasterSource: SortedKeyValueIterator[Key, Value] = null
  protected var topKey: Key = null
  protected var topValue: Value = null
  protected var nextKey: Key = null
  protected var nextValue: Value = null
  protected var nextFeature: SimpleFeature = null

  protected var filter: org.opengis.filter.Filter = null
  protected var decoder: IndexCQMetadataDecoder = null
  protected var testSimpleFeature: SimpleFeature = null
  protected var dateAttributeName: Option[String] = None

  protected var deduplicate: Boolean = false

  // each batch-scanner thread maintains its own (imperfect!) list of the
  // unique (in-polygon) identifiers it has seen
  protected var maxInMemoryIdCacheEntries = 10000
  protected val inMemoryIdCache = new JHashSet[String]()

  override def init(source: SortedKeyValueIterator[Key, Value], options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    TServerClassLoader.initClassLoader(logger)

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    val featureType = SimpleFeatureTypes.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)
    featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    dateAttributeName = getDtgFieldName(featureType)

    // default to text if not found for backwards compatibility
    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)

    decoder  = IndexSchema.getIndexMetadataDecoder(schemaEncoding)

    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME)) {
      val filterString  = options.get(DEFAULT_FILTER_PROPERTY_NAME)
      filter = ECQL.toFilter(filterString)
      val sfb = new SimpleFeatureBuilder(featureType)
      testSimpleFeature = sfb.buildFeature("test")
    }

    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
    deduplicate = IndexSchema.mayContainDuplicates(featureType)

    this.rasterSource = source.deepCopy(env)
  }

  def seekData(decodedMetadata: IndexEntry.DecodedCQMetadata): Unit = {
    // Is this needed?
    // do we care about the DecodedCQMetadata at this point at all?
    val rasterSourceTopKey = rasterSource.getTopKey
    val rasterSourceTopVal = rasterSource.getTopValue
    // what else is needed here?
    nextKey = new Key(rasterSourceTopKey)
    nextValue = new Value(rasterSourceTopVal)
  }

  /**
   * Position the index-source.  Consequently, updates the data-source.
   *
   * @param range
   * @param columnFamilies
   * @param inclusive
   */
  def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    // move the source iterator to the right starting spot
    rasterSource.seek(range, columnFamilies, inclusive)
    // find the first index-entry that is inside the search polygon
    // (use the current entry, if it's already inside the search polygon)
    findTop()
    // pre-fetch the next entry, if one exists
    // (the idea is to always be one entry ahead)
    if (nextKey != null) next()
  }


  def findTop(): Unit = {
    // clear out the reference to the next entry
    nextKey = null
    nextValue = null
    while (nextValue == null && rasterSource.hasTop && rasterSource.getTopKey != null) {
      // only consider this raster entry if we can decode the metadata
      decodeKey(rasterSource.getTopKey).map { decodedKey =>
        // the value contains the full-resolution geometry and time; use them
        lazy val decodedMetadata = IndexEntry.decodeIndexCQMetadata(rasterSource.getTopKey)
        lazy val isSTAcceptable = wrappedSTFilter(decodedMetadata.geom, decodedMetadata.dtgMillis)
        // see whether this box is acceptable
        // (the tests are ordered from fastest to slowest to take advantage of
        // short-circuit evaluation)
        if (isIdUnique(decodedMetadata.id) && isSTAcceptable) {
          // stash this ID
          rememberId(decodedMetadata.id)
          // use our seekData function to seek to next key
          seekData(decodedMetadata)
        }
      }
      // once you are done with the current nextKey/nextValue, move forward
      // you MUST advance to the next key
      rasterSource.next()
    }
  }

  override def hasTop: Boolean =  nextKey != null || topKey != null

  override def getTopKey: Key = topKey

  override def getTopValue: Value = topValue

  override def next(): Unit = {
    if (nextKey == null) {
      // this means that there are no more data to return
      topKey = null
      topValue = null
    } else {
      // assume the previously found values
      topKey = nextKey
      topValue = nextValue
      // does it make sense to have it in this order?
      findTop()
    }
  }

  /**
   * Attempt to decode the given key.  This should only succeed in the cases
   * where the key corresponds to an index-entry (not a data-entry).
   *
   * todo: fix, This is not utilizing the CQ meta data!
   */
  def decodeKey(key:Key): Option[SimpleFeature] = Try(decoder.decode(key)).toOption

  def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("RFI does not support deepCopy.")

  // NB: This is duplicated in the AIFI.  Consider refactoring.
  lazy val wrappedSTFilter: (Geometry, Option[Long]) => Boolean = {
    if (filter != null && testSimpleFeature != null) {
      (geom: Geometry, olong: Option[Long]) => {
        testSimpleFeature.setDefaultGeometry(geom)
        for {
          dateAttribute <- dateAttributeName
          long <- olong
        } {
          testSimpleFeature.setAttribute(dateAttribute, new Date(long))
        }
        filter.evaluate(testSimpleFeature)
      }
    } else {
      (_, _) => true
    }
  }

  /**
   * Returns a local estimate as to whether the current identifier
   * is likely to be a duplicate.
   *
   * Because we set a limit on how many unique IDs will be preserved in
   * the local cache, a TRUE response is always accurate, but a FALSE
   * response may not be accurate.  (That is, this cache allows for false-
   * negatives, but no false-positives.)  We accept this, because there is
   * a final, client-side filter that will eliminate all duplicate IDs
   * definitively.  The purpose of the local cache is to reduce traffic
   * through the remainder of the iterator/aggregator pipeline as quickly as
   * possible.
   *
   * @return False if this identifier is in the local cache; True otherwise
   */
  lazy val isIdUnique: (String) => Boolean =
    if (deduplicate) (id:String) => (id!=null) && !inMemoryIdCache.contains(id)
    else                       _ => true

  lazy val rememberId: (String) => Unit =
    if (deduplicate) (id: String) => {
      if (id!=null && !inMemoryIdCache.contains(id) && inMemoryIdCache.size < maxInMemoryIdCacheEntries)
        inMemoryIdCache.add(id)
    } else _ => Unit

}

object RasterFilteringIterator extends IteratorHelpers { }