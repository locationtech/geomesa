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

import java.io.{Serializable => JSerializable}
import java.util.{Date, Map => JMap}

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.TestWithDataStore
import org.locationtech.geomesa.core.index.QueryHints
import org.locationtech.geomesa.feature.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.io.Source
import scala.languageFeature.postfixOps
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class QuerySizeIteratorTest extends Specification with TestWithDataStore {

  sequential

  override def spec = "dtg:Date,geom:Point:srid=4326"

  val testData : Map[String,String] = {
    val source = Source.fromInputStream(getClass.getResourceAsStream("/test-lines.tsv"))
    val map = source.getLines().toList.map(_.split("\t")).map(l => (l(0) -> l(1))).toMap
    source.close()
    map
  }

  def getQuery(query: String): Query = {
    val q = new Query(sftName, ECQL.toFilter(query))
    q.getHints.put(QueryHints.QUERY_SIZE_KEY, java.lang.Boolean.TRUE)
    q
  }

  val randomSeed = 62

  "QuerySizeIterator" should {

    "calculate correct aggregated totals" in {
      val feats = (0 until 150).map { i =>
        val id = i.toString
        val dtg = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate
        val geom = "POINT(" + (-180 + i ) + " " +  (90-i) + ")"
        ScalaSimpleFeatureFactory.buildFeature(sft, Array(dtg, geom), id)
      }

      val otherFeats = (-90 until 90).map { i => // Add extra points around the boundary to check proper filtering
        val id = (i+151).toString
        val dtg = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate
        val geom = "POINT(-101 " + i + ")"
        ScalaSimpleFeatureFactory.buildFeature(sft, Array(dtg, geom), id)
      }

      clearFeatures()
      addFeatures(feats)
      addFeatures(otherFeats)


      val q = getQuery("BBOX(geom, -100, 1, 0, 40)")
      val results = fs.getFeatures(q).features().toList
      results must haveLength(1)

      val result = results.head

      val scanSizeBytes = result.getAttribute(QuerySizeIterator.SCAN_BYTES_ATTRIBUTE).asInstanceOf[Long]
      val scanRecords = result.getAttribute(QuerySizeIterator.SCAN_RECORDS_ATTRIBUTE).asInstanceOf[Long]
      val scanKeyBytes = result.getAttribute(QuerySizeIterator.SCAN_KEY_BYTES_ATTRIBUTE).asInstanceOf[Long]
      val returnSizeBytes = result.getAttribute(QuerySizeIterator.RESULT_BYTES_ATTRIBUTE).asInstanceOf[Long]
      val returnRecords = result.getAttribute(QuerySizeIterator.RESULT_RECORDS_ATTRIBUTE).asInstanceOf[Long]
      val returnKeyBytes = result.getAttribute(QuerySizeIterator.RESULT_KEY_BYTES_ATTRIBUTE).asInstanceOf[Long]

      val expectedNumRecords = 10
      val currentExpectedSizeInBytes = 30
      val currentExpectedKeySizeInBytes = 44

      scanRecords should be greaterThan expectedNumRecords
      scanSizeBytes should be greaterThan returnSizeBytes
      scanKeyBytes should be greaterThan returnKeyBytes
      // Give an expanded range on Byte sizes
      returnSizeBytes should be greaterThan (currentExpectedSizeInBytes-1)*expectedNumRecords
      returnSizeBytes should be lessThan (currentExpectedSizeInBytes+1)*expectedNumRecords
      returnKeyBytes should be greaterThan (currentExpectedKeySizeInBytes-1)*expectedNumRecords
      returnKeyBytes should be lessThan (currentExpectedKeySizeInBytes+1)*expectedNumRecords
      returnRecords should be equalTo expectedNumRecords
    }
  }
}

