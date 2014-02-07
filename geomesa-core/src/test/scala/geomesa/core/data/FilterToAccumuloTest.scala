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

package geomesa.core.data

import collection.immutable.TreeSet
import com.vividsolutions.jts.geom.Polygon
import geomesa.core.data.FilterToAccumulo.{SetLikeFilter, SetLikeInterval, SetLikePolygon}
import geomesa.core.index._
import geomesa.utils.text.WKTUtils
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterToAccumuloTest extends Specification {
  val dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()

  val MinDate = SpatioTemporalIndexSchema.minDateTime
  val MaxDate = SpatioTemporalIndexSchema.maxDateTime

  // re-used test data
  val date = "2010-01-01T00:00:00.000Z"
  val dateEarly = "2010-01-01T00:00:00.000Z"
  val dateLate = "2010-01-31T23:59:59.999Z"
  val dateEarly2 = "2010-01-15T00:00:00.000Z"
  val dateLate2 = "2010-02-15T23:59:59.999Z"
  val dateEarlyLow = "2010-01-01T00:00:00.000Z"
  val dateEarlyHigh = "2010-01-01T00:01:00.000Z"


  case class ExtractionResult(polygon: Option[Polygon], interval: Option[Interval], ecqlOut: String)

  def extract(ecqlIn: String, defaultPolygon: Polygon = null,
              defaultInterval: Interval = null,
              defaultFilter: Filter = Filter.INCLUDE): ExtractionResult = {

    val extractor = FilterExtractor(SF_PROPERTY_GEOMETRY, TreeSet(SF_PROPERTY_START_TIME, SF_PROPERTY_END_TIME))
    val extraction = extractor.extractAndModify(ECQL.toFilter(ecqlIn)).getOrElse(
      Extraction(
        SetLikePolygon.nothing,
        SetLikeInterval.nothing,
        SetLikeFilter.nothing
      )
    )
    val ecqlOut = extraction match {
      case null => null
      case _ => ECQL.toCQL(extraction.filter.getOrElse(Filter.INCLUDE))
    }
    ExtractionResult(extraction.polygon, extraction.interval, ecqlOut)
  }

  def normalizedCQL(cql: String): String = ECQL.toCQL(ECQL.toFilter(cql))

  "extraction/modification of filter with polygon, without interval" should {
    "behave correctly" in {
      val ecqlIn = s"(foo <= 1.34 AND BBOX($SF_PROPERTY_GEOMETRY, 1, 2, 3, 4))"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo true
      WKTUtils.write(polygon.get) must be equalTo "POLYGON ((1 2, 1 4, 3 4, 3 2, 1 2))"
      SetLikeInterval.isUndefined(interval) must be equalTo true
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }

  "extraction/modification of filter with polygon OR polygon, without interval" should {
    "behave correctly" in {
      val ecqlIn = s"(foo <= 1.34 AND (BBOX($SF_PROPERTY_GEOMETRY, 1, 3, 2, 4) OR BBOX($SF_PROPERTY_GEOMETRY, 2, 3, 3, 4)))"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo true
      WKTUtils.write(polygon.get) must be equalTo "POLYGON ((1 3, 1 4, 2 4, 3 4, 3 3, 2 3, 1 3))"
      SetLikeInterval.isDefined(interval) must be equalTo false
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }

  "extraction/modification of filter without polygon, with interval AFTER" should {
    "behave correctly" in {
      val ecqlIn = s"(INCLUDE AND ($SF_PROPERTY_START_TIME AFTER $date)) AND (foo <= 1.34)"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo false
      SetLikeInterval.isDefined(interval) must be equalTo true
      interval.get.toString must be equalTo s"$date/${SpatioTemporalIndexSchema.maxDateTime}"
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }

  "extraction/modification of filter without polygon, with interval BEFORE" should {
    "behave correctly" in {
      val ecqlIn = s"(INCLUDE AND ($SF_PROPERTY_START_TIME BEFORE $date)) AND (foo <= 1.34)"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo false
      SetLikeInterval.isDefined(interval) must be equalTo true
      interval.get.toString must be equalTo s"${SpatioTemporalIndexSchema.minDateTime}/$date"
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }

  "extraction/modification of filter without polygon, with interval BETWEEN" should {
    "behave correctly" in {
      val ecqlIn = s"(INCLUDE AND ($SF_PROPERTY_START_TIME BETWEEN $dateEarly AND $dateLate)) AND (foo <= 1.34)"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo false
      SetLikeInterval.isDefined(interval) must be equalTo true
      interval.get.toString must be equalTo s"$dateEarly/$dateLate"
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }

  "extraction/modification of filter with polygon OR polygon, with interval DURING" should {
    "behave correctly" in {
      val ecqlIn = s"($SF_PROPERTY_START_TIME DURING $dateEarly/$dateLate) AND (foo <= 1.34 AND (BBOX($SF_PROPERTY_GEOMETRY, 1, 3, 2, 4) OR BBOX($SF_PROPERTY_GEOMETRY, 2, 3, 3, 4)))"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo true
      WKTUtils.write(polygon.get) must be equalTo "POLYGON ((1 3, 1 4, 2 4, 3 4, 3 3, 2 3, 1 3))"
      SetLikeInterval.isDefined(interval) must be equalTo true
      interval.get.toString must be equalTo s"$dateEarly/$dateLate"
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }

  "extraction/modification of filter with polygon OR polygon, with interval DURING OR interval DURING" should {
    "behave correctly" in {
      val ecqlIn = s"(($SF_PROPERTY_START_TIME DURING $dateEarly2/$dateLate2) OR $SF_PROPERTY_START_TIME DURING $dateEarly/$dateLate) AND (foo <= 1.34 AND (BBOX($SF_PROPERTY_GEOMETRY, 1, 3, 2, 4) OR BBOX($SF_PROPERTY_GEOMETRY, 2, 3, 3, 4)))"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo true
      WKTUtils.write(polygon.get) must be equalTo "POLYGON ((1 3, 1 4, 2 4, 3 4, 3 3, 2 3, 1 3))"
      SetLikeInterval.isDefined(interval) must be equalTo true
      interval.get.toString must be equalTo s"$dateEarly/$dateLate2"
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }

  "extraction/modification of filter with polygon AND polygon, with interval DURING AND interval DURING" should {
    "behave correctly" in {
      val ecqlIn = s"(($SF_PROPERTY_START_TIME DURING $dateEarly2/$dateLate2) AND $SF_PROPERTY_START_TIME DURING $dateEarly/$dateLate) AND (foo <= 1.34 AND (BBOX($SF_PROPERTY_GEOMETRY, 1, 2, 5, 4) AND BBOX($SF_PROPERTY_GEOMETRY, 2, 0, 7, 3)))"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo true
      WKTUtils.write(polygon.get) must be equalTo "POLYGON ((5 3, 5 2, 2 2, 2 3, 5 3))"
      SetLikeInterval.isDefined(interval) must be equalTo true
      interval.get.toString must be equalTo s"$dateEarly2/$dateLate"
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }

  "extraction/modification of OR filter with polygon AND polygon, with interval DURING AND interval DURING" should {
    "behave correctly" in {
      val ecqlIn = s"(($SF_PROPERTY_START_TIME DURING $dateEarly2/$dateLate2) AND $SF_PROPERTY_START_TIME DURING $dateEarly/$dateLate) OR (foo <= 1.34 AND (BBOX($SF_PROPERTY_GEOMETRY, 1, 3, 2, 4) AND BBOX($SF_PROPERTY_GEOMETRY, 2, 3, 3, 4)))"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      // this query is of the form:  { temporal constraint } OR { spatial constraint }
      // this doesn't boil down to a single, simple query-(polygon|interval) pair;
      // for this type of query, just pass everything through as ECQL

      SetLikePolygon.isDefined(polygon) must be equalTo false
      SetLikeInterval.isDefined(interval) must be equalTo false
      ecqlOut must be equalTo normalizedCQL(ecqlIn)
    }
  }

  "AND with more than two children" should {
    "behave correctly" in {
      val fieldName = "ID"
      val quote = "\""
      val quotedFieldName = s"$quote$fieldName$quote"
      val ecqlIn = s"(INCLUDE AND ( $quotedFieldName = 88481 ) AND ( $SF_PROPERTY_START_TIME BETWEEN $dateEarly AND $dateLate ) AND BBOX ($SF_PROPERTY_GEOMETRY, 1, 3, 2, 4))"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo true
      WKTUtils.write(polygon.get) must be equalTo "POLYGON ((1 3, 1 4, 2 4, 2 3, 1 3))"
      SetLikeInterval.isDefined(interval) must be equalTo true
      interval.get.toString must be equalTo s"$dateEarly/$dateLate"
      ecqlOut must be equalTo s"$fieldName = 88481"
    }
  }

  "EQUALS on date" should {
    "behave correctly" in {
      val ecqlIn = s"(INCLUDE AND ($SF_PROPERTY_START_TIME = $dateEarly)) AND (foo <= 1.34)"
      val ExtractionResult(polygon, interval, ecqlOut) = extract(ecqlIn)

      SetLikePolygon.isDefined(polygon) must be equalTo false
      SetLikeInterval.isDefined(interval) must be equalTo true
      interval.get.toString must be equalTo s"$dateEarlyLow/$dateEarlyHigh"
      ecqlOut must be equalTo "foo <= 1.34"
    }
  }
}
