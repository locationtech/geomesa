/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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


package geomesa.core.iterators

import java.util.Date

import com.google.common.collect.HashBasedTable
import com.vividsolutions.jts.geom.{Envelope, Point}
import geomesa.core.data.AccumuloDataStoreFactory
import geomesa.core.index.{Constants, QueryHints}
import geomesa.feature.AvroSimpleFeatureFactory
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.io.Text
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DensityIteratorTest extends Specification {

  import geomesa.utils.geotools.Conversions._

  val spec = "id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Point:srid=4326"
  val sft = DataUtilities.createType("test", spec)
  sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")

  def createDataStore(i: Int = 0): DataStore = {
    val mockInstance = new MockInstance("dummy" + i)
    val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))
    c.tableOperations.create("test")
    val splits = (0 to 99).map {
      s => "%02d".format(s)
    }.map(new Text(_))
    c.tableOperations().addSplits("test", new java.util.TreeSet[Text](splits))

    val dsf = new AccumuloDataStoreFactory

    import geomesa.core.data.AccumuloDataStoreFactory.params._

    val ds = dsf.createDataStore(
                Map(
                     zookeepersParam.key -> "dummy",
                     instanceIdParam.key -> ("dummy" + i),
                     userParam.key -> "user",
                     passwordParam.key -> "pass",
                     tableNameParam.key -> "test",
                     mockParam.key -> "true"
                   ))
    ds.createSchema(sft)
    ds
  }

  def loadFeatures(ds: DataStore, encodedFeatures: Array[_<:Array[_]]): SimpleFeatureStore = {
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
    q.getHints.put(QueryHints.DENSITY_KEY, java.lang.Boolean.TRUE)
    q.getHints.put(QueryHints.BBOX_KEY, new ReferencedEnvelope(geom, DefaultGeographicCRS.WGS84))
    q.getHints.put(QueryHints.WIDTH_KEY, 500)
    q.getHints.put(QueryHints.HEIGHT_KEY, 500)
    q
  }

  "DensityIterator" should {
    "reduce total features returned" in {

      val ds = createDataStore(0)

      val encodedFeatures = (0 until 150).toArray.map {
        i =>
          Array(s"$i", "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }

      val fs = loadFeatures(ds, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = fs.getFeatures(q)

      val iter = results.features().toList
      iter must not beNull

      iter.length should be lessThan 150
    }

    "maintain total weight of points" in {

      val ds = createDataStore(1)

      val encodedFeatures = (0 until 150).toArray.map {
        i =>
          Array(s"$i", "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }

      val fs = loadFeatures(ds, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = fs.getFeatures(q)

      val iter = results.features().toList
      iter must not beNull

      val total = iter.map(_.getAttribute("weight").asInstanceOf[Double]).sum

      total should be equalTo 150
    }

    "maintain weights irrespective of dates" in {

      val ds = createDataStore(2)

      val encodedFeatures = (0 until 150).toArray.map {
        i =>
          val date = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate
          Array(s"$i", "1.0", new Date(date.getTime + i * 60000), "POINT(-77 38)")
      }

      val fs = loadFeatures(ds, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = fs.getFeatures(q)

      val iter = results.features().toList
      iter must not beNull

      val total = iter.map(_.getAttribute("weight").asInstanceOf[Double]).sum

      total should be equalTo 150
    }

    "correctly bin points" in {

      val ds = createDataStore(3)

      val encodedFeatures = (0 until 150).toArray.map {
        i =>
          val date = new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate
          // space out the points very slightly around 5 primary latitudes 1 degree apart
          val lat = (i/30).toInt + 1 + (Random.nextDouble() - 0.5) / 1000.0
          Array(s"$i", "1.0", new Date(date.getTime + i * 60000), s"POINT($lat 37)")
      }

      val fs = loadFeatures(ds, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -1, 33, 6, 40)")

      val results = fs.getFeatures(q)

      val iter = results.features().toList
      iter must not beNull

      val total = iter.map(_.getAttribute("weight").asInstanceOf[Double]).sum

      total should be equalTo 150

      val compiled = iter.groupBy(_.getAttribute("geom").asInstanceOf[Point])
        .map(entry => (entry._1, entry._2.map(_.getAttribute("weight").asInstanceOf[Double]).sum))

      // should be 5 bins of 30
      compiled.size should be equalTo 5
      compiled.forall(entry => entry._2 == 30) should be equalTo true
    }

    "encode and decode features" in {

      var matrix = HashBasedTable.create[Double, Double, Long]()
      matrix.put(1.0, 2.0, 3)
      matrix.put(2.0, 3.0, 5)

      val encoded = DensityIterator.encodeSparseMatrix(matrix)

      val decoded = DensityIterator.decodeSparseMatrix(encoded)

      matrix should be equalTo decoded

      println()
    }
  }

}