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

import collection.JavaConversions._
import collection.JavaConverters._
import com.vividsolutions.jts.geom.{Polygon, Geometry}
import geomesa.core._
import geomesa.core.data.SimpleFeatureEncoderFactory
import geomesa.core.index._
import geomesa.utils.text.WKTUtils
import java.util
import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{IteratorSetting, Connector, BatchWriterConfig}
import org.apache.accumulo.core.data._
import org.apache.hadoop.io.Text
import org.geotools.data.{DataUtilities, Query}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{Interval, DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.util.{Try, Random}

@RunWith(classOf[JUnitRunner])
class SpatioTemporalIntersectingIteratorTest extends Specification {

  val TEST_USER = "root"
  val TEST_TABLE = "test_table"
  val TEST_AUTHORIZATIONS = Constants.NO_AUTHS

  sequential

  def getRandomSuffix: String = {
    val chars = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

    (1 to 20).map(i => chars(Random.nextInt(chars.size))).mkString
  }

  val emptyBytes = new Value(Array[Byte]())

  object UnitTestEntryType  {
    def getTypeSpec = "POINT:String," + "LINESTRING:String," + "POLYGON:String," + "attr2:String," + spec
  }

  object TestData {

    case class Entry(wkt: String, id: String, dt: DateTime = new DateTime(defaultDateTime))

    // set up the geographic query polygon
    val wktQuery = "POLYGON((45 23, 48 23, 48 27, 45 27, 45 23))"

    val featureEncoder = SimpleFeatureEncoderFactory.defaultEncoder
    val featureName = "feature"
    val schemaEncoding = "%~#s%" + featureName + "#cstr%10#r%0,1#gh%yyyyMM#d::%~#s%1,3#gh::%~#s%4,3#gh%ddHH#d%10#id"
    val featureType: SimpleFeatureType = DataUtilities.createType(featureName, UnitTestEntryType.getTypeSpec)
    featureType.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

    val index = IndexSchema(schemaEncoding, featureType, featureEncoder)

    val defaultDateTime = new DateTime(2011, 6, 1, 0, 0, 0, DateTimeZone.forID("UTC")).toDate

    // utility function that can encode multiple types of geometry
    def createObject(id: String, wkt: String, dt: DateTime = null): List[(Key, Value)] = {
      val geomType: String = wkt.split( """\(""").head
      val geometry: Geometry = WKTUtils.read(wkt)
      val entry = SimpleFeatureBuilder.build(featureType, List(null, null, null, null, geometry, dt.toDate, dt.toDate), s"|data|$id")
      entry.setAttribute(geomType, id)
      entry.setAttribute("attr2", "2nd" + id)
      index.encode(entry).toList
    }

    val points = List[Entry](
      Entry("POINT(47.2 25.6)", "1"), // hit
      Entry("POINT(17.2 35.6)", "2"),
      Entry("POINT(87.2 15.6)", "3"),
      Entry("POINT(47.2 25.6)", "4"), // hit
      Entry("POINT(17.2 22.6)", "5"),
      Entry("POINT(-47.2 -25.6)", "6"),
      Entry("POINT(47.2 25.6)", "7"), // hit
      Entry("POINT(67.2 -25.6)", "8"),
      Entry("POINT(47.2 28.0)", "9"),
      Entry("POINT(47.2 25.6)", "10"), // hit
      Entry("POINT(47.2 25.6)", "11"), // hit
      Entry("POINT(47.2 25.6)", "12"), // hit
      Entry("POINT(47.2 25.6)", "13"), // hit
      Entry("POINT(50.2 30.6)", "14"),
      Entry("POINT(50.2 30.6)", "15"),
      Entry("POINT(50.2 30.6)", "16"),
      Entry("POINT(50.2 30.6)", "17"),
      Entry("POINT(50.2 30.6)", "18"),
      Entry("POINT(50.2 30.6)", "19"),
      Entry("POINT(47.2 25.6)", "20"), // hit
      Entry("POINT(47.2 25.6)", "21"), // hit
      Entry("POINT(47.2 25.6)", "22"), // hit
      Entry("POINT(47.2 25.6)", "23"), // hit
      Entry("POINT(47.2 25.6)", "24"), // hit
      Entry("POINT(47.2 25.6)", "25"), // hit
      Entry("POINT(47.2 25.6)", "26"), // hit
      Entry("POINT(47.2 25.6)", "27"), // hit
      Entry("POINT(47.2 25.6)", "111"), // hit
      Entry("POINT(47.2 25.6)", "112"), // hit
      Entry("POINT(47.2 25.6)", "113"), // hit
      Entry("POINT(50.2 30.6)", "114"),
      Entry("POINT(50.2 30.6)", "115"),
      Entry("POINT(50.2 30.6)", "116"),
      Entry("POINT(50.2 30.6)", "117"),
      Entry("POINT(50.2 30.6)", "118"),
      Entry("POINT(50.2 30.6)", "119")
    )

    // add some lines to this query, both qualifying and non-qualifying
    val lines = List(
      Entry("LINESTRING(47.28515625 25.576171875, 48 26, 49 27)", "201"),
      Entry("LINESTRING(-47.28515625 -25.576171875, -48 -26, -49 -27)", "202")
    )

    // add some polygons to this query, both qualifying and non-qualifying
    // NOTE:  Only the last of these will match the ColF set, because they tend
    //        to be decomposed into 15-bit (3-character) GeoHash cells.
    val polygons = List(
      Entry("POLYGON((44 24, 44 28, 49 27, 49 23, 44 24))", "301"),
      Entry("POLYGON((-44 -24, -44 -28, -49 -27, -49 -23, -44 -24))", "302"),
      Entry("POLYGON((47.28515625 25.576171875, 47.28515626 25.576171876, 47.28515627 25.576171875, 47.28515625 25.576171875))", "303")
    )

    val fullData = points ::: lines ::: polygons

    val hugeData: List[Entry] = {
      val rng = new Random(0)
      val minTime = new DateTime(2010, 6, 1, 0, 0, 0, DateTimeZone.forID("UTC")).getMillis
      val maxTime = new DateTime(2010, 8, 31, 23, 59, 59, DateTimeZone.forID("UTC")).getMillis
      (1 to 50000).map(i => {
        val wkt = "POINT(" +
          (40.0 + 10.0 * rng.nextDouble()).toString + " " +
          (20.0 + 10.0 * rng.nextDouble()).toString + " " +
          ")"
        val dt = new DateTime(
          math.round(minTime + (maxTime - minTime) * rng.nextDouble()),
          DateTimeZone.forID("UTC")
        )
        Entry(wkt, (100000 + i).toString, dt)
      }).toList
    }

    val pointWithNoID = List(Entry("POINT(-78.0 38.0)", null))

    val shortListOfPoints = List[Entry](
      Entry("POINT(47.2 25.6)", "1"), // hit
      Entry("POINT(47.2 25.6)", "7"), // hit
      Entry("POINT(50.2 30.6)", "117"),
      Entry("POINT(50.2 30.6)", "118"),
      Entry("POINT(47.2 25.6)", "4")
    )

    // this point's geohash overlaps with the query polygon so is a candidate result
    // however, the point itself is outside of the candidate result
    val geohashHitActualNotHit = List(Entry("POINT(47.999962 22.999969)", "9999"))

    def encodeDataList(entries: List[Entry] = fullData): util.Collection[(Key, Value)] = {
      val list: List[(Key, Value)] =
        entries.flatMap { entry =>
            createObject(entry.id, entry.wkt, entry.dt)
        }.toList

      list.sortWith((kvA: (Key, Value), kvB: (Key, Value)) => kvA._1.toString < kvB._1.toString).asJavaCollection
    }

    def encodeDataMap(entries: List[Entry] = fullData): util.TreeMap[Key, Value] = {
      val list = encodeDataList(entries)

      val map = new util.TreeMap[Key, Value]()
      list.foreach(kv => map(kv._1) = kv._2)

      map
    }


    def setupMockAccumuloTable(entries: List[Entry], numExpected: Int): Connector = {
      val mockInstance = new MockInstance()
      val c = mockInstance.getConnector(TEST_USER, new PasswordToken(Array[Byte]()))
      c.tableOperations.create(TEST_TABLE)
      val bw = c.createBatchWriter(TEST_TABLE, new BatchWriterConfig)

      // populate the mock table
      val dataList: util.Collection[(Key, Value)] = TestData.encodeDataList(entries)
      dataList.map { case (key, value) =>
        val m: Mutation = new Mutation(key.getRow)
        m.put(key.getColumnFamily, key.getColumnQualifier, value)
        bw.addMutation(m)
      }

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
  }

  def runMockAccumuloTest(label: String,
                          entries: List[TestData.Entry] = TestData.fullData,
                          ecqlFilter: Option[String] = None,
                          numExpectedDataIn: Int = 113,
                          dtFilter: Interval = null,
                          overrideGeometry: Boolean = false,
                          doPrint: Boolean = true): Int = {

    val featureEncoder = SimpleFeatureEncoderFactory.defaultEncoder

    // create the schema, and require de-duplication
    val schema = IndexSchema(TestData.schemaEncoding, TestData.featureType, featureEncoder)

    // create the query polygon
    val polygon: Polygon = overrideGeometry match {
      case true => IndexSchema.everywhere
      case false => WKTUtils.read(TestData.wktQuery).asInstanceOf[Polygon]
    }

    // create the batch scanner
    val c = TestData.setupMockAccumuloTable(entries, numExpectedDataIn)
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
      val numHits: Int = runMockAccumuloTest("mock-small", entries, None, 5)

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
        TestData.fullData, None, 113)

      // validate the total number of query-hits
      numHits must be equalTo (21)
    }
  }

  "Realistic Mock Accumulo with a meaningless attribute-filter" should {
    "return a full results-set" in {
      val ecqlFilter = "true = true"

      // run this query on regular data
      val numHits: Int = runMockAccumuloTest("mock-attr-all",
        TestData.fullData, Some(ecqlFilter), 113)

      // validate the total number of query-hits
      numHits must be equalTo (21)
    }
  }

  "Realistic Mock Accumulo with a meaningful attribute-filter" should {
    "return a partial results-set" in {
      val ecqlFilter = """(attr2 like '2nd___')"""

      // run this query on regular data
      val numHits: Int = runMockAccumuloTest("mock-attr-filt",
        TestData.fullData, Some(ecqlFilter), 113)

      // validate the total number of query-hits
      numHits must be equalTo (6)
    }
  }

  "Realistic Mock Accumulo" should {
    "handle edge intersection false positives" in {
      val numHits = runMockAccumuloTest("mock-small", TestData.shortListOfPoints ++ TestData.geohashHitActualNotHit, None, 6)
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
        TestData.hugeData, Some(ecqlFilter), TestData.hugeData.size)

      // validate the total number of query-hits
      numHits must be equalTo (68)
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
        TestData.hugeData, Some(ecqlFilter), TestData.hugeData.size, dtFilter)

      // validate the total number of query-hits
      numHits must be equalTo (68)
    }
  }

  "Large Mock Accumulo with a degenerate time-range" should {
    "return a filterd results-set" in {
      val ecqlFilter = "true = true"

      // run this query on regular data
      val dtFilter = IndexSchema.everywhen
      val numHits: Int = runMockAccumuloTest("mock-huge-notime",
        TestData.hugeData, Some(ecqlFilter), TestData.hugeData.size, dtFilter,
        doPrint = false)

      // validate the total number of query-hits
      numHits must be equalTo (6027)
    }
  }

  "Large Mock Accumulo with a global request" should {
    "return an unfiltered results-set" in {
      // run this query on regular data
      val dtFilter = IndexSchema.everywhen
      val numHits: Int = runMockAccumuloTest("mock-huge-notime",
        TestData.hugeData, None, TestData.hugeData.size, dtFilter,
        overrideGeometry = true, doPrint = false)

      // validate the total number of query-hits
      numHits must be equalTo (50000)
    }
  }

  "Consistency Iterator" should {
    "verify consistency of table" in {
      val c = TestData.setupMockAccumuloTable(TestData.shortListOfPoints, TestData.shortListOfPoints.length)
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
      val c = TestData.setupMockAccumuloTable(TestData.shortListOfPoints, TestData.shortListOfPoints.length)
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
      val c = Try(TestData.setupMockAccumuloTable(TestData.pointWithNoID, TestData.pointWithNoID.length))

      c.isFailure must be equalTo false
    }
  }
}
