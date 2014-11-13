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

import com.vividsolutions.jts.geom.Envelope
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.io.Text
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.{Constants, QueryHints}
import org.locationtech.geomesa.core.iterators.TemporalDensityIterator.{ENCODED_TIME_SERIES, decodeTimeSeries}
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class TemporalDensityIteratorTest extends Specification {

  sequential

  import org.locationtech.geomesa.utils.geotools.Conversions._

  def createDataStore(sft: SimpleFeatureType, i: Int = 0): DataStore = {
    val testTableName = "tdi_test"

    val mockInstance = new MockInstance("dummy" + i)
    val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))
    c.tableOperations.create(testTableName)
    val splits = (0 to 99).map {
      s => "%02d".format(s)
    }.map(new Text(_))
    c.tableOperations().addSplits(testTableName, new java.util.TreeSet[Text](splits))

    val dsf = new AccumuloDataStoreFactory

    import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._

    val ds = dsf.createDataStore(Map(
      zookeepersParam.key -> "dummy",
      instanceIdParam.key -> f"dummy$i%d",
      userParam.key       -> "user",
      passwordParam.key   -> "pass",
      tableNameParam.key  -> testTableName,
      mockParam.key       -> "true"))
    ds.createSchema(sft)
    ds
  }

  def loadFeatures(ds: DataStore, sft: SimpleFeatureType, encodedFeatures: Array[_ <: Array[_]]): SimpleFeatureStore = {
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    val features = encodedFeatures.map {
      e =>
        val f = builder.buildFeature(e(0).toString, e.asInstanceOf[Array[AnyRef]])
        f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        f.getUserData.put(Hints.PROVIDED_FID, e(0).toString)
        f
    }

    val fs = ds.getFeatureSource("test").asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(DataUtilities.collection(features))
    fs.getTransaction.commit()
    fs
  }

  def getQuery(query: String): Query = {
    val q = new Query("test", ECQL.toFilter(query))
    val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
    q.getHints.put(QueryHints.TEMPORAL_DENSITY_KEY, java.lang.Boolean.TRUE)
    q.getHints.put(QueryHints.TIME_INTERVAL_KEY, new Interval(new DateTime("2012-01-01T0:00:00", DateTimeZone.UTC).getMillis, new DateTime("2012-01-02T0:00:00", DateTimeZone.UTC).getMillis))
    q.getHints.put(QueryHints.TIME_BUCKETS_KEY, 24)
    q
  }

  "TemporalDensityIterator" should {
    val spec = "id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Geometry:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", spec)
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")

    val ds = createDataStore(sft,0)
    val encodedFeatures = (0 until 150).toArray.map{
      i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    }
    val fs = loadFeatures(ds, sft, encodedFeatures)

    "reduce total features returned" in {
      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")
      val results = fs.getFeatures(q)
      val allFeatures = results.features()
      val iter = allFeatures.toList
      (iter must not).beNull

      iter.length should be lessThan 150
      iter.length should be equalTo 1
    }

    "maintain total weights of time" in {

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = fs.getFeatures(q)
      val iter = results.features().toList
      val sf = iter.head.asInstanceOf[SimpleFeature]
      iter must not beNull

      val timeSeries = decodeTimeSeries(sf.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])
      val totalCount = timeSeries.map { case (dateTime, count) => count}.sum

      totalCount should be equalTo 150
      timeSeries.size should be equalTo 1
    }

    "maintain total irrespective of point" in {
      val ds = createDataStore(sft, 1)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, s"POINT(-77.$i 38.$i)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = fs.getFeatures(q)
      val sfList = results.features().toList

      val sf = sfList.head.asInstanceOf[SimpleFeature]
      val timeSeries = decodeTimeSeries(sf.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])

      val total = timeSeries.map { case (dateTime, count) => count}.sum

      total should be equalTo 150
      timeSeries.size should be equalTo 1
    }

    "correctly bin off of time intervals" in {
      val ds = createDataStore(sft, 2)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")


      val results = fs.getFeatures(q)
      val sf = results.features().toList.head.asInstanceOf[SimpleFeature]
      val timeSeries = decodeTimeSeries(sf.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])

      val total = timeSeries.map {
        case (dateTime, count) =>
          count should be equalTo 2L
          count}.sum

      total should be equalTo 48
      timeSeries.size should be equalTo 24
    }

    "encode decode feature" in {
      var timeSeries = new collection.mutable.HashMap[DateTime, Long]()
      timeSeries.put(new DateTime("2012-01-01T00:00:00", DateTimeZone.UTC), 2)
      timeSeries.put(new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC), 8)

      val encoded = TemporalDensityIterator.encodeTimeSeries(timeSeries)
      val decoded = TemporalDensityIterator.decodeTimeSeries(encoded)

      timeSeries should be equalTo decoded
      timeSeries.size should be equalTo 2
      timeSeries.get(new DateTime("2012-01-01T00:00:00", DateTimeZone.UTC)).get should be equalTo 2L
      timeSeries.get(new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC)).get should be equalTo 8L
    }

    "query dtg bounds not in DataStore" in {
      val ds = createDataStore(sft, 3)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-02-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = fs.getFeatures(q)
      val sfList = results.features().toList
      sfList.length should be equalTo 0
    }

    "nothing to query over" in {
      val ds = createDataStore(sft, 4)
      val encodedFeatures = new Array[Array[_]](0)
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = fs.getFeatures(q)
      val sfList = results.features().toList
      sfList.length should be equalTo 0
    }
  }
}
