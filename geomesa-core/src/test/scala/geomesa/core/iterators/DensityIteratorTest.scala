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

import geomesa.feature.AvroSimpleFeatureFactory

import collection.JavaConversions._
import com.vividsolutions.jts.geom.Envelope
import geomesa.core.data.AccumuloDataStoreFactory
import geomesa.core.index.{QueryHints, Constants}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.io.Text
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{Query, DataUtilities}
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DensityIteratorTest extends Specification {

  import geomesa.utils.geotools.Conversions._

  "DensityIterator" should {
    "compute densities" in {
      val mockInstance = new MockInstance("dummy")
      val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))
      c.tableOperations.create("test")
      val splits = (0 to 99).map {
        s => "%02d".format(s)
      }.map(new Text(_))
      c.tableOperations().addSplits("test", new java.util.TreeSet[Text](splits))

      val dsf = new AccumuloDataStoreFactory

      import AccumuloDataStoreFactory.params._

      val ds = dsf.createDataStore(
        Map(
          zookeepersParam.key -> "dummy",
          instanceIdParam.key -> "dummy",
          userParam.key -> "user",
          passwordParam.key -> "pass",
          tableNameParam.key -> "test",
          mockParam.key -> "true"
        ))


      val spec = "id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Point:srid=4326"
      val sft = DataUtilities.createType("test", spec)
      sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")

      ds.createSchema(sft)

      val encodedFeatures = (0 until 150).map {
        i =>
          Array(s"$i", "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }

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

      val q = new Query("test", ECQL.toFilter("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)"))
      val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
      q.getHints.put(QueryHints.DENSITY_KEY, java.lang.Boolean.TRUE)
      q.getHints.put(QueryHints.BBOX_KEY, new ReferencedEnvelope(geom, DefaultGeographicCRS.WGS84))
      q.getHints.put(QueryHints.WIDTH_KEY, 600)
      q.getHints.put(QueryHints.HEIGHT_KEY, 600)
      val results = fs.getFeatures(q)

      val iter = results.features().toList
      iter must not beNull
    }
  }

}