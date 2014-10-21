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

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.data.tables.AttributeTable._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor

import scala.util.{Failure, Success}

/**
 * This is an Attribute Index Only Iterator. It should be used to avoid a join on the records table
 * in cases where only the geom, dtg and attribute in question are needed.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 */
class AttributeIndexIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  var indexSource: SortedKeyValueIterator[Key, Value] = null

  var topKey: Option[Key] = None
  var topValue: Option[Value] = None

  // the following fields get filled in during init
  var dtgFieldName: Option[String] = null
  var attributeRowPrefix: String = null
  var attributeType: Option[AttributeDescriptor] = null
  var featureBuilder: SimpleFeatureBuilder = null
  var featureEncoder: SimpleFeatureEncoder = null

  var filterTest: (Geometry, Option[Long]) => Boolean = (_, _) => true

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {

    TServerClassLoader.initClassLoader(logger)

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    // we need the original SFT name in order to decode correctly due to table sharing
    val featureType = SimpleFeatureTypes
        .createType(options.get(GEOMESA_ITERATORS_SFT_NAME), simpleFeatureTypeSpec)
    featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    dtgFieldName = getDtgFieldName(featureType)
    attributeRowPrefix = index.getTableSharingPrefix(featureType)
    // if we're retrieving the attribute, we need the class in order to decode it
    attributeType = Option(options.get(GEOMESA_ITERATORS_ATTRIBUTE_NAME))
        .flatMap(n => Option(featureType.getDescriptor(n)))

    // default to text if not found for backwards compatibility
    val encoding = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    featureEncoder = SimpleFeatureEncoder(featureType, encoding)

    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(featureType)

    // combine the simpleFeatureFilteringIterator functionality so we only have to decode each row once
    Option(options.get(DEFAULT_FILTER_PROPERTY_NAME)).foreach { filterString =>
      val filter = ECQL.toFilter(filterString)

      val sfb = new SimpleFeatureBuilder(featureType)
      val testFeature = sfb.buildFeature("test")

      filterTest = (geom: Geometry, odate: Option[Long]) => {
        testFeature.setDefaultGeometry(geom)
        dtgFieldName.foreach(dtgField => odate.foreach(date => testFeature.setAttribute(dtgField, new Date(date))))
        filter.evaluate(testFeature)
      }
    }

    this.indexSource = source.deepCopy(env)
  }

  override def hasTop = topKey.isDefined

  override def getTopKey = topKey.orNull

  override def getTopValue = topValue.orNull

  /**
   * Seeks to the start of a range and fetches the top key/value
   *
   * @param range
   * @param columnFamilies
   * @param inclusive
   */
  override def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    // move the source iterator to the right starting spot
    indexSource.seek(range, columnFamilies, inclusive)
    findTop()
  }

  /**
   * Reads the next qualifying key/value
   */
  override def next() = findTop()

  /**
   * Advances the index-iterator to the next qualifying entry
   */
  def findTop() {

    // clear out the reference to the last entry
    topKey = None
    topValue = None

    // loop while there is more data and we haven't matched our filter
    while (topValue.isEmpty && indexSource.hasTop) {

      // the value contains the full-resolution geometry and time
      val decodedValue = IndexEntry.decodeIndexValue(indexSource.getTopValue)

      if (filterTest(decodedValue.geom, decodedValue.dtgMillis)) {
        // current entry matches our filter - update the key and value
        // copy the key because reusing it is UNSAFE
        topKey = Some(new Key(indexSource.getTopKey))
        // using the already decoded index value, generate a SimpleFeature
        val sf = IndexIterator.encodeIndexValueToSF(featureBuilder,
                                                    decodedValue.id,
                                                    decodedValue.geom,
                                                    decodedValue.dtgMillis)

        // if they requested the attribute value, decode it from the row key
        if (attributeType.isDefined) {
          val row = topKey.get.getRow.toString
          val decoded = decodeAttributeIndexRow(attributeRowPrefix, attributeType.get, row)
          decoded match {
            case Success(att) => sf.setAttribute(att.attributeName, att.attributeValue)
            case Failure(e) => logger.error(s"Error decoding attribute row: row: $row, error: ${e.toString}")
          }
        }

        // set the encoded simple feature as the value
        topValue = Some(new Value(featureEncoder.encode(sf)))
      }

      // increment the underlying iterator
      indexSource.next()
    }
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("AttributeIndexIterator does not support deepCopy.")
}


