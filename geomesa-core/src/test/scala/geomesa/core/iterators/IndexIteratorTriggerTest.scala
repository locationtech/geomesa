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
import com.vividsolutions.jts.geom.{Polygon, Geometry}
import geomesa.core.data._
import geomesa.core.index._
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
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}
import org.specs2.runner.JUnitRunner
import scala.Some
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.geotools.filter.Filter

@RunWith(classOf[JUnitRunner])
class IndexIteratorTriggerTest extends Specification {
  sequential


  object TestTable {
    val TEST_TABLE = "test_table"
    val featureName = "feature"
    val schemaEncoding = "%~#s%" + featureName + "#cstr%10#r%0,1#gh%yyyyMM#d::%~#s%1,3#gh::%~#s%4,3#gh%ddHH#d%10#id"

    val testFeatureTypeSpec: String = {
      "POINT:String," + "LINESTRING:String," + "POLYGON:String," + "attr2:String," + spec
    }

    def testFeatureType: SimpleFeatureType = {
      val featureType: SimpleFeatureType = DataUtilities.createType(featureName, testFeatureTypeSpec)
      featureType.getUserData.put(SF_PROPERTY_START_TIME, "geomesa_index_start_time")
      featureType
    }


    def sampleQuery(ecql: org.opengis.filter.Filter, finalAttributes: Array[String] ): Query = {
      val aQuery = new Query(testFeatureType.getTypeName, ecql, finalAttributes)
      val fs = TestTable.setupMockFeatureSource
      fs.getFeatures(aQuery) // only used to mutate the state of aQuery. yuck.
      aQuery
    }


    def setupMockFeatureSource: SimpleFeatureStore = {
      val mockInstance = new MockInstance("dummy")
      val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))
      if (c.tableOperations.exists(TEST_TABLE)) c.tableOperations.delete(TEST_TABLE)

      val dsf = new AccumuloDataStoreFactory

      import AccumuloDataStoreFactory.params._

      val ds = dsf.createDataStore(
        Map(
          zookeepersParam.key -> "dummy",
          instanceIdParam.key -> "dummy",
          userParam.key -> "user",
          passwordParam.key -> "pass",
          authsParam.key -> "S,USA",
          tableNameParam.key -> "test_table",
          mockParam.key -> "true",
          featureEncParam.key -> "avro",
          idxSchemaParam.key -> schemaEncoding
        ))

      ds.createSchema(testFeatureType)
      val fs = ds.getFeatureSource(featureName).asInstanceOf[SimpleFeatureStore]
      //val dataFeatures = convertToSimpleFeatures(entries)
      //val featureCollection = DataUtilities.collection(dataFeatures)
      //fs.addFeatures(featureCollection)
      //fs.getTransaction.commit()
      fs
    }

    def simpleECQL = {
      //val filterString = "(not " + SF_PROPERTY_START_TIME +
      //" after 2010-08-08T23:59:59Z) and (not " + SF_PROPERTY_START_TIME +
      //" before 2010-08-08T00:00:00Z)"
      val filterString = "true = true"
      //val filterString = " [[ geomesa_index_geometry within POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90)) ] AND [ geomesa_index_start_time BETWEEN 0000-01-01T00:00:00.000Z AND 9999-12-31T23:59:59.000Z ]] "
      ECQL.toFilter(filterString)
    }
  }
  object TriggerTest {
    def simpleECQL: org.opengis.filter.Filter = {
      //val filterString = "(not " + SF_PROPERTY_START_TIME +
      //" after 2010-08-08T23:59:59Z) and (not " + SF_PROPERTY_START_TIME +
      //" before 2010-08-08T00:00:00Z)"
      val filterString = "true = true"
      //val filterString = " [[ geomesa_index_geometry within POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90)) ] AND [ geomesa_index_start_time BETWEEN 0000-01-01T00:00:00.000Z AND 9999-12-31T23:59:59.000Z ]] "
      ECQL.toFilter(filterString)
    }
    def simpleTransform = {
      //Array("geomesa_index_start_time", "geomesa_index_geometry")
      Array("geomesa_index_geometry")
    }
  }

  /**
   * useIndexOnlyIterator (ecqlPredicate: Option[String], query: Query, sourceSFTSpec: String)
   *    getFilterAttributes(ecql_text: String)
   *     passThroughFilter(ecql_text: String)
   *     filterOnIndexAttributes(ecql_text: String, schema: SimpleFeatureType)
   *     isOneToOneTransformation(transformDefs: String, schema: SimpleFeatureType)
   *      isIdentityTransformation(transformDefs: String)
   *       transformAttributes(transform_text: String)
   *        transformOnIndexAttributes(transform_text: String, sourceSchema: SimpleFeatureType)
   *        generateTransformedSimpleFeature(ecqlPredicate: Option[String], query: Query, sourceSFTSpec: String)
   *        useIndexOnlyIterator(ecqlPredicate: Option[String], query: Query, sourceSFTSpec: String)
   *        implicit class IndexAttributeNames(sft: SimpleFeatureType) {
    def geoName = sft.getGeometryDescriptor.getLocalName
    // must use this logic if the UserData may not be present in the SimpleFeatureType
    def startTimeName =  attributeNameHandler(SF_PROPERTY_START_TIME)
    def endTimeName   =  attributeNameHandler(SF_PROPERTY_END_TIME)
    def attributeNameHandler(attributeKey: String): Option[String]
    def indexAttributeNames = List(geoName) ++ startTimeName ++ endTimeName
   */
    /**
    "IndexAttributeNames" should {
      "handle attribute names" in {
        generateTransformedSimpleFeature(ecqlPredicate: Option[String], query: Query, sourceSFTSpec: String)
      }
    }
    **/

    "useIndexOnlyIterator" should {
      "be run when requesting only index attributes" in {
        val ecqlPred = Some(TriggerTest.simpleECQL.toString)
        val aQuery = TestTable.sampleQuery(TriggerTest.simpleECQL,TriggerTest.simpleTransform)
        val isTriggered = IndexIteratorTrigger.useIndexOnlyIterator(ecqlPred, aQuery, TestTable.testFeatureTypeSpec)
        isTriggered must beTrue
      }
    }
    "transformOnindexAttributes" should{
      "return true when requesting only index attributes" in {
        val retVal= IndexIteratorTrigger.transformOnIndexAttributes(TriggerTest.simpleTransform.toString, TestTable.testFeatureType)
        retVal must beTrue
      }
    }
    "filterOnIndexAttributes(ep, sourceSFT)" should {
      "return true when dealing with do nothing filter" in {
        val retVal = IndexIteratorTrigger.filterOnIndexAttributes(TestTable.simpleECQL.toString,TestTable.testFeatureType )
        retVal must beTrue
      }
    }
}