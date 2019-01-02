/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.time.{ZoneOffset, ZonedDateTime}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Polygon
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.iterators.TestData._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class MultiIteratorTest extends Specification with TestWithMultipleSfts with LazyLogging {

  sequential

  val spec = SimpleFeatureTypes.encodeType(TestData.featureType, includeUserData = true)

  val MinDateTime = ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  val MaxDateTime = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)

  // noinspection LanguageFeature
  // note: size returns an estimated amount, instead we need to actually count the features
  implicit def collectionToIter(c: SimpleFeatureCollection): SelfClosingIterator[SimpleFeature] = SelfClosingIterator(c)

  def getQuery(sft: SimpleFeatureType,
               ecqlFilter: Option[String],
               dtFilter: (ZonedDateTime, ZonedDateTime) = null,
               overrideGeometry: Boolean = false): Query = {
    val polygon: Polygon = if (overrideGeometry) {
      org.locationtech.geomesa.utils.geotools.WholeWorldPolygon
    } else {
      WKTUtils.read(TestData.wktQuery).asInstanceOf[Polygon]
    }

    val gf = s"INTERSECTS(geom, ${polygon.toText})"
    val dt: Option[String] = Option(dtFilter).map { case (start, end) =>
      s"(dtg between '$start' AND '$end')"
    }

    def red(f: String, og: Option[String]) = og match {
      case Some(g) => s"$f AND $g"
      case None => f
    }

    val tfString = red(red(gf, dt), ecqlFilter)
    val tf = ECQL.toFilter(tfString)

    new Query(sft.getTypeName, tf)
  }

  def output(f: Filter, filterCount: Int, queryCount: Int): Unit = {
    if (filterCount != queryCount) {
      logger.error(s"Filter: $f expected: $filterCount query: $queryCount")
    } else {
      logger.debug(s"Filter: $f expected: $filterCount query: $queryCount")
    }
  }

  "Mock Accumulo with fullData" should {
    val sft = createNewSchema(spec)
    val features = TestData.fullData.map(createSF(_, sft))
    addFeatures(sft, features)
    val fs = ds.getFeatureSource(sft.getTypeName)

    "return the same result for our iterators" in {
      val q = getQuery(sft, None)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      stQueriedCount mustEqual filteredCount
    }

    "return a full results-set" in {
      val filterString = "true = true"

      val q = getQuery(sft, Some(filterString))

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      // validate the total number of query-hits
      stQueriedCount mustEqual filteredCount
    }

    "return a partial results-set" in {
      val filterString = """(attr2 like '2nd___')"""

      val q = getQuery(sft, Some(filterString))

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      // validate the total number of query-hits
      stQueriedCount mustEqual filteredCount
    }
  }

  "Mock Accumulo with a small table" should {
    val sft = createNewSchema(spec)
    val features = TestData.shortListOfPoints.map(createSF(_, sft))
    addFeatures(sft, features)
    val fs = ds.getFeatureSource(sft.getTypeName)

    "cover corner cases" in {
      val q = getQuery(sft, None)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      // validate the total number of query-hits
      // Since we are playing with points, we can count **exactly** how many results we should
      //  get back.  This is important to check corner cases.
      stQueriedCount mustEqual filteredCount
    }
  }

  "Realistic Mock Accumulo" should {
    val sft = createNewSchema(spec)
    val features = (TestData.shortListOfPoints ++ TestData.geohashHitActualNotHit).map(createSF(_, sft))
    addFeatures(sft, features)
    val fs = ds.getFeatureSource(sft.getTypeName)

    "handle edge intersection false positives" in {
      val q = getQuery(sft, None)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      // validate the total number of query-hits
      stQueriedCount mustEqual filteredCount
    }
  }

  "Large Mock Accumulo" should {
    val sft = createNewSchema(spec)
    val features = TestData.hugeData.map(createSF(_, sft))
    addFeatures(sft, features)
    val fs = ds.getFeatureSource(sft.getTypeName)

    "return a partial results-set with a meaningful attribute-filter" in {
      val filterString = "(not dtg after 2010-08-08T23:59:59Z) and (not dtg_end_time before 2010-08-08T00:00:00Z)"

      val q = getQuery(sft, Some(filterString))

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      // validate the total number of query-hits
      stQueriedCount mustEqual filteredCount
    }

    "return a filtered results-set with a meaningful time-range" in {
      val filterString = "true = true"

      val dtFilter = (
        ZonedDateTime.of(2010, 8, 8, 0, 0, 0, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2010, 8, 8, 23, 59, 59, 999000000, ZoneOffset.UTC)
      )

      val q = getQuery(sft, Some(filterString), dtFilter)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      // validate the total number of query-hits
      stQueriedCount mustEqual filteredCount
    }

    "return a filtered results-set with a degenerate time-range" in {
      val filterString = "true = true"

      val dtFilter = (MinDateTime, MaxDateTime)
      val q = getQuery(sft, Some(filterString), dtFilter)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      // validate the total number of query-hits
      stQueriedCount mustEqual filteredCount
    }

    "return an unfiltered results-set with a global request" in {
      val dtFilter = (MinDateTime, MaxDateTime)
      val q = getQuery(sft, None, dtFilter, overrideGeometry = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).length

      output(q.getFilter, filteredCount, stQueriedCount)

      // validate the total number of query-hits
      stQueriedCount mustEqual filteredCount
    }
  }

  "non-point geometries" should {
    val sft = createNewSchema(spec)
    val wkts = Seq[String](
      "POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))",
      "POLYGON((-10 -10, -10 0, 0 0, 0 -10, -10 -10))",
      "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
      "POLYGON((-10 0, -10 10, 0 10, 0 0, -10 0))",
      "POLYGON((0 0, 10 0, 10 -10, 0 -10, 0 0))"
    )
    val features: Seq[SimpleFeature] = wkts.zipWithIndex.map {
      case (wkt, i) => createSF(Entry(wkt, s"fid_$i"), sft)
    }
    addFeatures(sft, features)
    val fs = ds.getFeatureSource(sft.getTypeName)

    def doesQueryRun(filterString: String, optExpectedCount: Option[Int] = None): Boolean = {
      logger.debug(s"Odd-point query filter:  $filterString")

      val outcome = Try {
        val q = getQuery(sft, Some(filterString), overrideGeometry = true)

        val filteredCount = features.count(q.getFilter.evaluate)
        val stQueriedCount = fs.getFeatures(q).length

        output(q.getFilter, filteredCount, stQueriedCount)

        val expectedCount = optExpectedCount.getOrElse(filteredCount)

        logger.debug(s"Query:\n  $filterString\n  Expected count:  $optExpectedCount -> $expectedCount" +
          s"\n  Filtered count:  $filteredCount\n  ST-queried count:  $stQueriedCount")

        // validate the total number of query-hits
        filteredCount == expectedCount && stQueriedCount == expectedCount
      }

      outcome match {
        case Success(result) => result
        case Failure(ex)     =>
          logger.error(ex.getStackTrace.mkString("\n"))
          false
      }
    }

    "perform query variants that include correctly" in {
      doesQueryRun("CONTAINS(geom, POINT(0.0 0.0))", Option(1)) must beTrue
      doesQueryRun("INTERSECTS(geom, POINT(0.0 0.0))") must beTrue
      doesQueryRun("INTERSECTS(POINT(0.0 0.0), geom)") must beTrue
    }
  }
}
