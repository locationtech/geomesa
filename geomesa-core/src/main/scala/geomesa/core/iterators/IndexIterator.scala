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

import com.vividsolutions.jts.geom._
import geomesa.core.data._
import geomesa.core.index._
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.DateTime
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * This is an Index Only Iterator, to be used in situations where the data records are
 * not useful enough to pay the penalty of decoding when using the
 * SpatioTemporalIntersectingIterator.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 *
 * Note that this extends the SpatioTemporalIntersectingIterator, but never creates a dataSource
 * and hence never iterates through it.
 */
class IndexIterator extends SpatioTemporalIntersectingIterator with SortedKeyValueIterator[Key, Value] {

  import geomesa.core._

  var featureBuilder: SimpleFeatureBuilder = null
  var featureEncoder: SimpleFeatureEncoder = null
  var outputAttributes: List[AttributeDescriptor] = null
  var indexAttributes: List[AttributeDescriptor] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {
    logger.trace("Initializing classLoader")
    IndexIterator.initClassLoader(logger)

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    val simpleFeatureType = DataUtilities.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)
    simpleFeatureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    featureEncoder = SimpleFeatureEncoderFactory.createEncoder(encodingOpt)

    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(simpleFeatureType)

    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
    decoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)

    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME)) {
      val filterString  = options.get(DEFAULT_FILTER_PROPERTY_NAME)
      filter = ECQL.toFilter(filterString)

      val featureType = DataUtilities.createType("DummyType", options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
      featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

      val sfb = new SimpleFeatureBuilder(featureType)
      geomTestSF = sfb.buildFeature("test")
    }

    if (options.containsKey(DEFAULT_INTERVAL_PROPERTY_NAME))
      interval = IndexIterator.decodeInterval(
        options.get(DEFAULT_INTERVAL_PROPERTY_NAME))

    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
    deduplicate = IndexSchema.mayContainDuplicates(simpleFeatureType)

    this.indexSource = source.deepCopy(env)
  }

  /**
   * Generates from the key's value a SimpleFeature that matches the current
   * (top) reference of the index-iterator.
   *
   * We emit the top-key from the index-iterator, and the top-value from the
   * converted key value.  This is *IMPORTANT*, as otherwise we do not emit rows
   * that honor the SortedKeyValueIterator expectation, and Bad Things Happen.
   */
  override def seekData(decodedValue: IndexSchema.DecodedIndexValue) {
    // now increment the value of nextKey, copy because reusing it is UNSAFE
    nextKey = new Key(indexSource.getTopKey)
    // using the already decoded index value, generate a SimpleFeature and set as the Value
    val nextSimpleFeature = IndexIterator.encodeIndexValueToSF(featureBuilder, decodedValue.id,
      decodedValue.geom, decodedValue.dtgMillis)
    nextValue = featureEncoder.encode(nextSimpleFeature)
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("IndexIterator does not support deepCopy.")
}

object IndexIterator extends IteratorHelpers {
  import IteratorTrigger.IndexAttributeNames

  /**
   * Converts values taken from the Index Value to a SimpleFeature, using the passed SimpleFeatureBuilder
   * Note that the ID, taken from the index, is preserved
   * Also note that the SimpleFeature's other attributes may not be fully parsed and may be left as null;
   * the SimpleFeatureFilteringIterator *may* remove the extraneous attributes later in the Iterator stack
   */
  def encodeIndexValueToSF(featureBuilder: SimpleFeatureBuilder, id: String,
                           geom: Geometry, dtgMillis: Option[Long]): SimpleFeature = {
    val theType = featureBuilder.getFeatureType
    val dtgDate = dtgMillis.map{time => new DateTime(time).toDate}
    // Build and fill the Feature. This offers some performance gain over building and then setting the attributes.
    featureBuilder.buildFeature(id, attributeArray(theType, geom, dtgDate ))
  }

  /**
   * Construct and fill an array of the SimpleFeature's attribute values
   */
  def attributeArray(theType: SimpleFeatureType, geomValue: Geometry, date: Option[java.util.Date]) = {
    val attrArray = new Array[AnyRef](theType.getAttributeCount)
    // always set the mandatory geo element
    attrArray(theType.indexOf(theType.geoName)) = geomValue
    // if dtgDT exists, attempt to fill the elements corresponding to the start and/or end times
    date.map{time => (theType.startTimeName ++ theType.endTimeName).map{name =>attrArray(theType.indexOf(name)) = time}}
    attrArray
  }
}