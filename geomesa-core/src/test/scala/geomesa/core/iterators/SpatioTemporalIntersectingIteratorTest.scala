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

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import geomesa.core._
import geomesa.core.data.SimpleFeatureEncoderFactory
import geomesa.core.index._
import geomesa.core.iterators.TestData._
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchScanner, BatchWriterConfig, Connector, IteratorSetting}
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.GenSeq
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Random, Try}

object UnitTestEntryType  {
  def getTypeSpec = "POINT:String," + "LINESTRING:String," + "POLYGON:String," + "attr2:String," + spec
}

@RunWith(classOf[JUnitRunner])
class SpatioTemporalIntersectingIteratorTest extends Specification with Logging {

  sequential

  def getRandomSuffix: String = {
    val chars = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

    (1 to 20).map(i => chars(Random.nextInt(chars.size))).mkString
  }

  def setupMockAccumuloTable(entries: GenSeq[Entry], tableName: String = TEST_TABLE): Connector = {
    val mockInstance = new MockInstance()
    val c = mockInstance.getConnector(TEST_USER, new PasswordToken(Array[Byte]()))
    c.tableOperations.create(tableName)
    val bw = c.createBatchWriter(tableName, new BatchWriterConfig)

    logger.debug(s"Add mutations to table $tableName.")
    for {
      entry        <- entries.par
      (key, value) <- createObject(entry.id, entry.wkt, entry.dt)
    } {
      val m: Mutation = new Mutation(key.getRow)
      m.put(key.getColumnFamily, key.getColumnQualifier, value)
      bw.addMutation(m)
    }

    logger.debug(s"Done adding mutations to table $tableName.")

    // add the schema description
    val mutSchema = new Mutation(s"~META_$featureName")
    mutSchema.put("schema", schemaEncoding, emptyBytes)
    bw.addMutation(mutSchema)

    // add the attributes description
    val mutAttributes = new Mutation(s"~META_$featureName")
    mutAttributes.put("attributes", UnitTestEntryType.getTypeSpec, emptyBytes)
    bw.addMutation(mutAttributes)

    bw.flush()
    c
  }

  def runMockAccumuloTest(label: String,
                          entries: GenSeq[TestData.Entry] = TestData.fullData,
                          ecqlFilter: Option[String] = None,
                          dtFilter: Interval = null,
                          overrideGeometry: Boolean = false,
                          doPrint: Boolean = true): Int = {
    // create the query polygon
    val polygon: Polygon = overrideGeometry match {
      case true => IndexSchema.everywhere
      case false => WKTUtils.read(TestData.wktQuery).asInstanceOf[Polygon]
    }

    // create the batch scanner
    val c = setupMockAccumuloTable(entries)
    val bs = () => c.createBatchScanner(TEST_TABLE, TEST_AUTHORIZATIONS, 5)

    val gf = s"WITHIN(geom, ${polygon.toText})"
    val dt: Option[String] = Option(dtFilter).map(int =>
      s"(dtg between '${int.getStart}' AND '${int.getEnd}')"
    )

    def red(f: String, og: Option[String]) = og match {
      case Some(g) => s"$f AND $g"
      case None => f
    }

    val tfString = red(red(gf, dt), ecqlFilter)
    val tf = ECQL.toFilter(tfString)

    val q = new Query(TestData.featureType.getTypeName, tf)
    runQuery(q, bs)
  }

  def runQuery(q: Query, bs: () => BatchScanner, doPrint: Boolean = true, label: String = "test") = {
    val featureEncoder = SimpleFeatureEncoderFactory.defaultEncoder
    // create the schema, and require de-duplication
    val schema = IndexSchema(TestData.schemaEncoding, TestData.featureType, featureEncoder)

    // fetch results from the schema!
    val itr = schema.query(q, bs)

    // print out the hits
    val retval = if (doPrint) {
      val results: List[SimpleFeature] = itr.toList
      results.map(simpleFeature => {
        val attrs = simpleFeature.getAttributes.map(attr => if (attr == null) "" else attr.toString).mkString("|")
        println("[SII." + label + "] query-hit:  " + simpleFeature.getID + "=" + attrs)
      })
      results.size
    } else itr.size

    itr.close()
    retval
  }

  "Mock Accumulo with a small table" should {
    "cover corner cases" in {
      // compose the list of entries to use
      val entries: List[TestData.Entry] = TestData.shortListOfPoints

      // run this query on regular data
      val numHits: Int = runMockAccumuloTest("mock-small", entries, None)

      // validate the total number of query-hits
      // Since we are playing with points, we can count **exactly** how many results we should
      //  get back.  This is important to check corner cases.
      numHits must be equalTo (3)
    }
  }

  "Realistic Mock Accumulo" should {
    "use our iterators and aggregators the same way we do" in {
      // run this query on regular data
      val numHits: Int = runMockAccumuloTest("mock-real",
        TestData.fullData, None)

      // validate the total number of query-hits
      numHits must be equalTo (21)
    }
  }

  "Realistic Mock Accumulo with a meaningless attribute-filter" should {
    "return a full results-set" in {
      val ecqlFilter = "true = true"

      // run this query on regular data
      val numHits: Int = runMockAccumuloTest("mock-attr-all",
        TestData.fullData, Some(ecqlFilter))

      // validate the total number of query-hits
      numHits must be equalTo (21)
    }
  }

  "Realistic Mock Accumulo with a meaningful attribute-filter" should {
    "return a partial results-set" in {
      val ecqlFilter = """(attr2 like '2nd___')"""

      // run this query on regular data
      val numHits: Int = runMockAccumuloTest("mock-attr-filt",
        TestData.fullData, Some(ecqlFilter))

      // validate the total number of query-hits
      numHits must be equalTo (6)
    }
  }

  "Realistic Mock Accumulo" should {
    "handle edge intersection false positives" in {
      val numHits = runMockAccumuloTest("mock-small", TestData.shortListOfPoints ++ TestData.geohashHitActualNotHit, None)
      numHits must be equalTo(3)
    }
  }

  "Large Mock Accumulo with a meaningful attribute-filter" should {
    "return a partial results-set" in {
      val ecqlFilter = "(not " + DEFAULT_DTG_PROPERTY_NAME +
        " after 2010-08-08T23:59:59Z) and (not " + DEFAULT_DTG_END_PROPERTY_NAME +
        " before 2010-08-08T00:00:00Z)"

      // run this query on regular data
      val numHits: Int = runMockAccumuloTest("mock-huge",
        TestData.hugeData, Some(ecqlFilter))

      // validate the total number of query-hits
      numHits must be equalTo 81
    }
  }

  "Large Mock Accumulo with a meaningful time-range" should {
    "return a filterd results-set" in {
      val ecqlFilter = "true = true"

      // run this query on regular data
      val dtFilter = new Interval(
        new DateTime(2010, 8, 8, 0, 0, 0, DateTimeZone.forID("UTC")),
        new DateTime(2010, 8, 8, 23, 59, 59, DateTimeZone.forID("UTC"))
      )
      val numHits: Int = runMockAccumuloTest("mock-huge-time",
        TestData.hugeData, Some(ecqlFilter), dtFilter)

      // validate the total number of query-hits
      numHits must be equalTo 81
    }
  }

  "Large Mock Accumulo with a degenerate time-range" should {
    "return a filterd results-set" in {
      val ecqlFilter = "true = true"

      // run this query on regular data
      val dtFilter = IndexSchema.everywhen
      val numHits: Int = runMockAccumuloTest("mock-huge-notime",
        TestData.hugeData, Some(ecqlFilter), dtFilter,
        doPrint = false)

      // validate the total number of query-hits
      numHits must be equalTo 7434
    }
  }

  "Large Mock Accumulo with a global request" should {
    "return an unfiltered results-set" in {
      // run this query on regular data
      val dtFilter = IndexSchema.everywhen
      val numHits: Int = runMockAccumuloTest("mock-huge-notime",
        TestData.hugeData, None, dtFilter,
        overrideGeometry = true, doPrint = false)

      // validate the total number of query-hits
      numHits must be equalTo 52000
    }
  }

  "Consistency Iterator" should {
    "verify consistency of table" in {
      val c = setupMockAccumuloTable(TestData.shortListOfPoints)
      val bs = c.createBatchScanner(TEST_TABLE, TEST_AUTHORIZATIONS, 8)
      val cfg = new IteratorSetting(1000, "consistency-iter", classOf[ConsistencyCheckingIterator])

      bs.setRanges(List(new org.apache.accumulo.core.data.Range()))
      bs.addScanIterator(cfg)

      // validate the total number of query-hits
      bs.iterator().size mustEqual(0)
    }
  }

  "Consistency Iterator" should {
    "verify inconsistency of table" in {
      val c = setupMockAccumuloTable(TestData.shortListOfPoints)
      val bd = c.createBatchDeleter(TEST_TABLE, TEST_AUTHORIZATIONS, 8, new BatchWriterConfig)
      bd.setRanges(List(new org.apache.accumulo.core.data.Range()))
      bd.fetchColumnFamily(new Text("|data|1".getBytes()))
      bd.delete()
      bd.flush()
      val bs = c.createBatchScanner(TEST_TABLE, TEST_AUTHORIZATIONS, 8)
      val cfg = new IteratorSetting(1000, "consistency-iter", classOf[ConsistencyCheckingIterator])

      bs.setRanges(List(new org.apache.accumulo.core.data.Range()))
      bs.addScanIterator(cfg)

      // validate the total number of query-hits
      bs.iterator().size mustEqual(1)
    }
  }

  "Feature with a null ID" should {
    "not fail to insert" in {
      val c = Try(setupMockAccumuloTable(TestData.pointWithNoID))

      c.isFailure must be equalTo false
    }
  }
}
