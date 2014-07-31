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

package geomesa.core.iterators

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Polygon
import geomesa.core._
import geomesa.core.data.AccumuloDataStoreFactory
import geomesa.core.index.{IndexSchema, IndexSchemaBuilder}
import geomesa.core.iterators.TestData._
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataUtilities, Query}
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.GenSeq
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class MultiIteratorTest extends Specification with Logging {

  object IteratorTest {
    def setupMockFeatureSource(entries: GenSeq[TestData.Entry], tableName: String = "test_table"): SimpleFeatureStore = {
      val mockInstance = new MockInstance("dummy")
      val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))

      // Remember we need to delete all 4 tables now
      List(
        tableName,
        s"${tableName}_${TestData.featureType.getTypeName}_st_idx",
        s"${tableName}_${TestData.featureType.getTypeName}_records",
        s"${tableName}_${TestData.featureType.getTypeName}_attr_idx"
      ).foreach { t => if (c.tableOperations.exists(t)) c.tableOperations.delete(t) }

      val dsf = new AccumuloDataStoreFactory

      import geomesa.core.data.AccumuloDataStoreFactory.params._

      val ds = dsf.createDataStore(Map(
        zookeepersParam.key -> "dummy",
        instanceIdParam.key -> "dummy",
        userParam.key       -> "user",
        passwordParam.key   -> "pass",
        authsParam.key      -> "S,USA",
        tableNameParam.key  -> tableName,
        mockParam.key       -> "true",
        featureEncParam.key -> "avro",
        idxSchemaParam.key  -> new IndexSchemaBuilder("~").randomNumber(3).constant("TEST").geoHash(0, 3).date("yyyyMMdd").nextPart().geoHash(3, 2).nextPart().id().build()))

      ds.createSchema(TestData.featureType)
      val fs = ds.getFeatureSource(TestData.featureName).asInstanceOf[SimpleFeatureStore]
      val dataFeatures = entries.par.map(createSF)
      val featureCollection = DataUtilities.collection(dataFeatures.toArray)
      fs.addFeatures(featureCollection)
      fs.getTransaction.commit()
      fs
    }
  }

  def getQuery(ecqlFilter: Option[String] = None,
               dtFilter: Interval = null,
               overrideGeometry: Boolean = false,
               indexIterator: Boolean = false): Query = {
    val polygon: Polygon = overrideGeometry match {
      case true => IndexSchema.everywhere
      case false => WKTUtils.read(TestData.wktQuery).asInstanceOf[Polygon]
    }

    val gf = s"INTERSECTS(geom, ${polygon.toText})"
    val dt: Option[String] = Option(dtFilter).map(int =>
      s"(dtg between '${int.getStart}' AND '${int.getEnd}')"
    )

    def red(f: String, og: Option[String]) = og match {
      case Some(g) => s"$f AND $g"
      case None => f
    }

    val tfString = red(red(gf, dt), ecqlFilter)
    val tf = ECQL.toFilter(tfString)

    if (indexIterator) {
      // select a few attributes to trigger the IndexIterator
      val outputAttributes = Array("geom", "dtg")
      new Query(TestData.featureType.getTypeName, tf, outputAttributes)
    } else {
      new Query(TestData.featureType.getTypeName, tf)
    }
  }

  "Mock Accumulo with fullData" should {
    val fs = IteratorTest.setupMockFeatureSource(TestData.fullData, "mock_full_data")
    val features = TestData.fullData.map(createSF)

    "return the same result for our iterators" in {
      val q = getQuery(None)
      val indexOnlyQuery = getQuery(indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }

    "return a full results-set" in {
      val filterString = "true = true"

      val q = getQuery(Some(filterString))
      val indexOnlyQuery = getQuery(Some(filterString), indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      // validate the total number of query-hits
      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }

    "return a partial results-set" in {
      val filterString = """(attr2 like '2nd___')"""

      val fs = IteratorTest.setupMockFeatureSource(TestData.fullData, "mock_attr_filt")
      val features = TestData.fullData.map(createSF)
      val q = getQuery(Some(filterString))
      val indexOnlyQuery = getQuery(Some(filterString), indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      // validate the total number of query-hits
      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }
  }


  "Mock Accumulo with a small table" should {
    "cover corner cases" in {
      val fs = IteratorTest.setupMockFeatureSource(TestData.shortListOfPoints, "mock_small_corner_cases")
      val features = TestData.shortListOfPoints.map(createSF)
      val q = getQuery(None)
      val indexOnlyQuery = getQuery(None, indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      // validate the total number of query-hits
      // Since we are playing with points, we can count **exactly** how many results we should
      //  get back.  This is important to check corner cases.
      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }
  }

  "Realistic Mock Accumulo" should {
    "handle edge intersection false positives" in {
      val fs = IteratorTest.setupMockFeatureSource(TestData.shortListOfPoints ++ TestData.geohashHitActualNotHit, "mock_small")
      val features = (TestData.shortListOfPoints ++ TestData.geohashHitActualNotHit).map(createSF)
      val q = getQuery(None)
      val indexOnlyQuery = getQuery(None, indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      // validate the total number of query-hits
      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }
  }

  "Large Mock Accumulo" should {
    val fs = IteratorTest.setupMockFeatureSource(TestData.hugeData, "mock_huge")
    val features = TestData.hugeData.map(createSF)

    "return a partial results-set with a meaningful attribute-filter" in {
      val filterString = "(not " + DEFAULT_DTG_PROPERTY_NAME +
        " after 2010-08-08T23:59:59Z) and (not " + DEFAULT_DTG_END_PROPERTY_NAME +
        " before 2010-08-08T00:00:00Z)"

      val q = getQuery(Some(filterString))
      val indexOnlyQuery = getQuery(Some(filterString), indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      // validate the total number of query-hits
      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }

    "return a filtered results-set with a meaningful time-range" in {
      val filterString = "true = true"

      val dtFilter = new Interval(
        new DateTime(2010, 8, 8, 0, 0, 0, DateTimeZone.forID("UTC")),
        new DateTime(2010, 8, 8, 23, 59, 59, DateTimeZone.forID("UTC"))
      )
      val fs = IteratorTest.setupMockFeatureSource(TestData.hugeData, "mock_huge_time")
      val features = TestData.hugeData.map(createSF)
      val q = getQuery(Some(filterString), dtFilter)
      val indexOnlyQuery = getQuery(Some(filterString), dtFilter, indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      // validate the total number of query-hits
      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }

    "return a filtered results-set with a degenerate time-range" in {
      val filterString = "true = true"

      val dtFilter = IndexSchema.everywhen
      val q = getQuery(Some(filterString), dtFilter)
      val indexOnlyQuery = getQuery(Some(filterString), dtFilter, indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      // validate the total number of query-hits
      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }

    "return an unfiltered results-set with a global request" in {
      val dtFilter = IndexSchema.everywhen
      val q = getQuery(None, dtFilter, overrideGeometry = true)
      val indexOnlyQuery = getQuery(None, dtFilter, overrideGeometry = true, indexIterator = true)

      val filteredCount = features.count(q.getFilter.evaluate)
      val stQueriedCount = fs.getFeatures(q).size
      val indexOnlyCount = fs.getFeatures(indexOnlyQuery).size

      logger.debug(s"Filter: ${q.getFilter} queryCount: $stQueriedCount filteredCount: $filteredCount indexOnlyCount: $indexOnlyCount")

      // validate the total number of query-hits
      indexOnlyCount mustEqual filteredCount
      stQueriedCount mustEqual filteredCount
    }
  }
}
