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
class QuerySizeteratorTest extends Specification with TestWithDataStore {

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
      val random = new Random(randomSeed)

      val feats = (0 until 150).map { i =>
        val id = i.toString
        //val map = Map("a" -> i, "b" -> i * 2, (if (random.nextBoolean()) "c" else "d") -> random.nextInt(10)).asJava
        val dtg = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate
        val geom = "POINT(" + (-180 + i ) + " " +  (90-i) + ")"
        ScalaSimpleFeatureFactory.buildFeature(sft, Array(dtg, geom), id)
      }

      val otherFeats = (-90 until 90).map { i => // Add extra points around the boundry to check proper filtering
        val id = (i+151).toString
        val dtg = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate
        val geom = "POINT(-101 " + i + ")"
        ScalaSimpleFeatureFactory.buildFeature(sft, Array(dtg, geom), id)
      }

      clearFeatures()
      addFeatures(feats)
      addFeatures(otherFeats)


//      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")
      val q = getQuery("BBOX(geom, -100, -100, 75, 75)")
      val results = fs.getFeatures(q).features().toList
      results must haveLength(1)  // JNH: This still makes sense.

      val result = results.head

      val scanSizeBytes = result.getAttribute(QuerySizeIterator.SCAN_BYTES_ATTRIBUTE).asInstanceOf[Long]
      val scanRecords = result.getAttribute(QuerySizeIterator.SCAN_RECORDS_ATTRIBUTE).asInstanceOf[Long]
      val returnSizeBytes = result.getAttribute(QuerySizeIterator.RESULT_BYTES_ATTRIBUTE).asInstanceOf[Long]
      val returnRecords = result.getAttribute(QuerySizeIterator.RESULT_RECORDS_ATTRIBUTE).asInstanceOf[Long]

      val expectedNumRecords = 70
      val currentExpectedSizeInBytes = 30
      println(scanSizeBytes + "\t" + scanRecords + "\t" + returnSizeBytes + "\t" + returnRecords)

      scanRecords should be greaterThan expectedNumRecords
      returnSizeBytes should be greaterThan (currentExpectedSizeInBytes-1)*expectedNumRecords
      returnSizeBytes should be lessThan (currentExpectedSizeInBytes+1)*expectedNumRecords // Give an expanded range on size
      returnRecords should be equalTo expectedNumRecords
    }

//    "correctly filter dates" in {
//      val random = new Random(randomSeed)
//
//      val feats = (0 until 200).map { i =>
//        val id = i.toString
//        val map = Map("a" -> i, "b" -> i * 2, (if (random.nextBoolean()) "c" else "d") -> random.nextInt(10)).asJava
//        val dtg = if (i < 100) new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate else new DateTime("2012-01-01T23:30:00", DateTimeZone.UTC).toDate
//        val geom = "POINT(-77 38)"
//        ScalaSimpleFeatureFactory.buildFeature(sft, Array(id, map, dtg, geom), id)
//      }
//
//      clearFeatures()
//      addFeatures(feats)
//
//      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")
//
//      val results = fs.getFeatures(q).features().toList
//      results must haveLength(1)
//
//      val aggregated = results.map(_.getAttribute("map").asInstanceOf[JMap[String, Int]].asScala).head
//
//      aggregated("a") should be equalTo (0 until 100).sum
//
//      aggregated("b") should be equalTo (0 until 100).map(_ * 2).sum
//
//      val c = aggregated("c")
//      c must beLessThanOrEqualTo(100 * 10)
//      c must beGreaterThanOrEqualTo(0)
//
//      val d = aggregated("d")
//      d must beLessThanOrEqualTo(100 * 10)
//      d must beGreaterThanOrEqualTo(0)
//    }
//
//    "do aggregation on a single feature" in {
//      val id = "0"
//      val map = Map("a" -> 0, "b" -> 0 * 2, "c" -> 9).asJava
//      val dtg = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate
//      val geom = testData("[POLYGON] Charlottesville")
//      val feature = ScalaSimpleFeatureFactory.buildFeature(sft, Array(id, map, dtg, geom), id)
//
//      clearFeatures()
//      addFeatures(Seq(feature))
//
//      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and " +
//          "BBOX(geom, -78.598118, 37.992204, -78.337364, 38.091238)")
//
//      val results = fs.getFeatures(q).features().toList
//      results must haveLength(1)
//
//      val aggregated = results.map(_.getAttribute("map").asInstanceOf[JMap[String, Int]].asScala).head
//
//      aggregated("a") should be equalTo 0
//
//      aggregated("b") should be equalTo 0
//
//      aggregated("c") should be equalTo 9
//    }
//
//    "handle no features" in {
//
//      clearFeatures()
//
//      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and " +
//          "BBOX(geom, -78.598118, 37.992204, -78.337364, 38.091238)")
//
//      val results = fs.getFeatures(q)
//
//      val features = results.features().toList
//      features must beEmpty
//    }
//
//    "do aggregation on a realistic set" in {
//
//      val date = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate
//      val feats = Seq[Array[AnyRef]](
//        Array("1", Map("a" -> 1, "b" -> 2, "c" -> 9).asJava, new Date(date.getTime + 1 * 60000), testData("[POLYGON] Charlottesville")),
//        Array("2", Map("a" -> 2, "b" -> 2, "d" -> 9).asJava, new Date(date.getTime + 2 * 60000), testData("[MULTIPOLYGON] test box")),
//        Array("3", Map("a" -> 3, "b" -> 2, "c" -> 3).asJava, new Date(date.getTime + 3 * 60000), testData("[LINE] test line")),
//        Array("4", Map("a" -> 4, "b" -> 2, "d" -> 3).asJava, new Date(date.getTime + 4 * 60000), testData("[LINE] Cherry Avenue segment")),
//        Array("5", Map("a" -> 5, "b" -> 2, "c" -> 6).asJava, new Date(date.getTime + 5 * 60000), testData("[MULTILINE] Cherry Avenue entirety"))
//      ).map(a => ScalaSimpleFeatureFactory.buildFeature(sft, a, a(0).asInstanceOf[String]))
//
//      clearFeatures()
//      addFeatures(feats)
//
//      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and " +
//          "BBOX(geom, -78.598118, 37.992204, -78.337364, 38.091238)")
//
//      val results = fs.getFeatures(q).features().toList
//      results must haveLength(1)
//
//      val aggregated = results.map(_.getAttribute("map").asInstanceOf[JMap[String, Int]].asScala).head
//
//      aggregated("a") mustEqual(10)
//      aggregated("b") mustEqual(6)
//      aggregated("c") mustEqual(15)
//      aggregated("d") mustEqual(3)
//    }
  }
}

