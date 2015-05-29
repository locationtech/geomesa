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

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    initFeatureType(options)
    super.init(featureType, options)

    featureBuilder = new SimpleFeatureBuilder(querySizeSFT)
    querySizeFeatureEncoder = SimpleFeatureEncoder(querySizeSFT, FeatureEncoding.KRYO)
  }

  // We understand the init/seek/hasTop/next lifecycle, so do this.
  // This iterator will only return one key-value pair which is computed during the 'seek'.
  override def next() = {
    topKey = null
    topValue = null
  }

  override def setTopConditionally() = {
    featureBuilder.reset()
    var scanBytes: Long = 0
    var scanRecords: Long = 0
    var scanKeyBytes: Long = 0
    var resultBytes: Long = 0
    var resultRecords: Long = 0
    var resultKeyBytes: Long = 0

    while (source.hasTop) {
      val keyBytes: Long = source.getTopKey.getLength
      val curNumBytes: Long = source.getTopValue.getSize
      topKey = source.getTopKey

      scanBytes += curNumBytes
      scanRecords += 1
      scanKeyBytes += keyBytes

      if (filter.evaluate(featureDecoder.decode(source.getTopValue.get))) {
        resultBytes += curNumBytes
        resultRecords += 1
        resultKeyBytes += keyBytes
      }
      source.next
    }

    featureBuilder.set(SCAN_BYTES_ATTRIBUTE, scanBytes)
    featureBuilder.set(SCAN_RECORDS_ATTRIBUTE, scanRecords)
    featureBuilder.set(SCAN_KEY_BYTES_ATTRIBUTE, scanKeyBytes)
    featureBuilder.set(RESULT_BYTES_ATTRIBUTE, resultBytes)
    featureBuilder.set(RESULT_RECORDS_ATTRIBUTE, resultRecords)
    featureBuilder.set(RESULT_KEY_BYTES_ATTRIBUTE, resultKeyBytes)

    if (scanRecords > 0) {
      topValue = new Value(querySizeFeatureEncoder.encode(featureBuilder.buildFeature("feature")))
    } else {
      topKey = null
      topValue = null
    }
  }

}

object QuerySizeIterator {
  val QUERY_SIZE_FEATURE_SFT_STRING =
    "geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date,"+
      "scanSizeBytes:Long,resultSizeBytes:Long,scanNumRecords:Long,resultNumRecords:Long,"+
      "scanKeyBytes:Long,resultKeyBytes:Long"
  val querySizeSFT = SimpleFeatureTypes.createType("querySize", QUERY_SIZE_FEATURE_SFT_STRING)
  val SCAN_BYTES_ATTRIBUTE = "scanSizeBytes"
  val SCAN_KEY_BYTES_ATTRIBUTE = "scanKeyBytes"
  val SCAN_RECORDS_ATTRIBUTE = "scanNumRecords"
  val RESULT_BYTES_ATTRIBUTE = "resultSizeBytes"
  val RESULT_RECORDS_ATTRIBUTE = "resultNumRecords"
  val RESULT_KEY_BYTES_ATTRIBUTE = "resultKeyBytes"
}
