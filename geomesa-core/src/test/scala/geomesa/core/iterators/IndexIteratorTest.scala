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

import geomesa.feature.AvroSimpleFeatureFactory

import collection.JavaConversions._
import com.vividsolutions.jts.geom.{Polygon, Geometry}
import geomesa.core.data._
import geomesa.core.index._
import geomesa.core.iterators.TestData._
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{Query, DataUtilities}
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{Duration, Interval, DateTime}
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.specs2.runner.JUnitRunner
import scala.collection.GenSeq

@RunWith(classOf[JUnitRunner])
class IndexIteratorTest extends SpatioTemporalIntersectingIteratorTest {

  import geomesa.utils.geotools.Conversions._
  import AccumuloDataStore._

  object IITest {

    def setupMockFeatureSource(entries: GenSeq[TestData.Entry]): SimpleFeatureStore = {
      val CATALOG_TABLE = "test_table"

      val mockInstance = new MockInstance("dummy")
      val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))

      // Remember we need to delete all 4 tables now
      List(
        CATALOG_TABLE,
        s"${CATALOG_TABLE}_${TestData.featureType.getTypeName}_st_idx",
        s"${CATALOG_TABLE}_${TestData.featureType.getTypeName}_records",
        s"${CATALOG_TABLE}_${TestData.featureType.getTypeName}_attr_idx"
      ).foreach { t => if (c.tableOperations.exists(t)) c.tableOperations.delete(t) }

      val dsf = new AccumuloDataStoreFactory

      import AccumuloDataStoreFactory.params._

      val ds = dsf.createDataStore(
        Map(
          zookeepersParam.key -> "dummy",
          instanceIdParam.key -> "dummy",
          userParam.key -> "user",
          passwordParam.key -> "pass",
          authsParam.key -> "S,USA",
          tableNameParam.key -> TEST_TABLE,
          mockParam.key -> "true",
          featureEncParam.key -> "avro",
          idxSchemaParam.key -> TestData.schemaEncoding
        ))

      ds.createSchema(TestData.featureType)
      val fs = ds.getFeatureSource(TestData.featureName).asInstanceOf[SimpleFeatureStore]
      val dataFeatures = entries.par.map(createSF)
      val featureCollection = DataUtilities.collection(dataFeatures.toArray)
      fs.addFeatures(featureCollection)
      fs.getTransaction.commit()
      fs
    }
  }

  override def runMockAccumuloTest(label: String,
                                   entries: GenSeq[TestData.Entry] = TestData.fullData,
                                   ecqlFilter: Option[String] = None,
                                   dtFilter: Interval = null,
                                   overrideGeometry: Boolean = false,
                                   doPrint: Boolean = true): Int = {
    logger.debug(s"Running test: $label")

    // create the query polygon
    val polygon: Polygon = overrideGeometry match {
      case true => IndexSchema.everywhere
      case false => WKTUtils.read(TestData.wktQuery).asInstanceOf[Polygon]
    }

    //create the Feature Source
    val fs = IITest.setupMockFeatureSource(entries)

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


    // select a few attributes to trigger the IndexIterator
    // Note that since we are re-running all the tests from the IntersectingIteratorTest,
    // some of the tests may actually use the IntersectingIterator
    val outputAttributes = Array("geom")
    //val q = new Query(TestData.featureType.getTypeName, tf)
    val q = new Query(TestData.featureType.getTypeName, tf, outputAttributes)
    val sfCollection = fs.getFeatures(q)
    sfCollection.features().count(x => true)
  }
}
