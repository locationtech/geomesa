/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

import java.util

import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.core
import org.locationtech.geomesa.core.iterators.QuerySizeIterator._
import org.locationtech.geomesa.feature.{FeatureEncoding, SimpleFeatureEncoder}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

/**
 * Iterator that returns counts and size-in-bytes of both results filtered and results returned.
 */
class QuerySizeIterator extends GeomesaFilteringIterator with HasFeatureDecoder with HasFilter with HasFeatureType {

  var featureBuilder: SimpleFeatureBuilder = null
  var querySizeFeatureEncoder: SimpleFeatureEncoder = null

  override def init(source: SortedKeyValueIterator[Key, Value], options: util.Map[String, String], env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    initFeatureType(options)
    // Need to grab the original SFT in order to properly decode/filter
    val originalSFT = SimpleFeatureTypes.createType("QuerySizeHijackedIteratorSFT",options.get(core.GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
    super[HasFilter].init(originalSFT, options)
    super[HasFeatureDecoder].init(originalSFT, options)

    featureBuilder = new SimpleFeatureBuilder(querySizeSFT)
    querySizeFeatureEncoder = SimpleFeatureEncoder(querySizeSFT, FeatureEncoding.KRYO)
  }

  // Could rewrite this to automatically scan to the next top, aggregating results as it goes.
  override def setTopConditionally() = {

    featureBuilder.reset()
    topKey = new Key(source.getTopKey)

    val curNumBytes: Long = source.getTopValue.get.length // Could this be wrong?

    featureBuilder.set(SCAN_BYTES_ATTRIBUTE, curNumBytes)
    featureBuilder.set(SCAN_RECORDS_ATTRIBUTE, 1)

    var resultBytes: Long = 0
    var resultRecords: Long = 0

    if (filter.evaluate(featureDecoder.decode(source.getTopValue.get))) {
      resultBytes = curNumBytes
      resultRecords = 1
    }
    featureBuilder.set(RESULT_BYTES_ATTRIBUTE, resultBytes)
    featureBuilder.set(RESULT_RECORDS_ATTRIBUTE, resultRecords)
    val feature = featureBuilder.buildFeature("feature")

    topValue = new Value(querySizeFeatureEncoder.encode(feature))
  }

}

object QuerySizeIterator {
  val QUERY_SIZE_FEATURE_SFT_STRING =  "geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date,scanSizeBytes:Long,resultSizeBytes:Long,scanNumRecords:Long,resultNumRecords:Long"
  val querySizeSFT = SimpleFeatureTypes.createType("querySize", QUERY_SIZE_FEATURE_SFT_STRING)
  val SCAN_BYTES_ATTRIBUTE = "scanSizeBytes"
  val SCAN_RECORDS_ATTRIBUTE = "scanNumRecords"
  val RESULT_BYTES_ATTRIBUTE = "resultSizeBytes"
  val RESULT_RECORDS_ATTRIBUTE = "resultNumRecords"
  val ORIGINAL_SFT_OPTION = "originalSFT"
}
