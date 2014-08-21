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

package org.locationtech.geomesa.core.index

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AttributeIndexStrategyTest extends Specification {

  sequential

  val sftName = "AttributeIndexStrategyTest"
  val spec = "name:String:index=true,age:Integer:index=true,count:Long:index=true," +
               "weight:Double:index=true,height:Float:index=true,admin:Boolean:index=true," +
               "geom:Geometry:srid=4326,dtg:Date:index=true"
  val sft = SimpleFeatureTypes.createType(sftName, spec)
  sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

  val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))

  val ds = DataStoreFinder.getDataStore(Map(
             "connector"   -> connector,
             // note the table needs to be different to prevent testing errors
             "tableName"   -> "AttributeIndexStrategyTest").asJava).asInstanceOf[AccumuloDataStore]

  ds.createSchema(sft, 2)

  val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)
  val builder = new SimpleFeatureBuilder(sft, new AvroSimpleFeatureFactory)
  val dtFormat = new SimpleDateFormat("yyyyMMdd HH:mm:SS")
  dtFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  case class TestAttributes(name: String,
                            age: Integer,
                            count: Long,
                            weight: Double,
                            height: Float,
                            admin: Boolean,
                            geom: Geometry,
                            dtg: Date)

  val geom = WKTUtils.read("POINT(45.0 49.0)")

  Seq(TestAttributes("alice",   20, 1, 5.0, 10.0F, true, geom, dtFormat.parse("20140101 12:00:00")),
      TestAttributes("bill",    21, 2, 6.0, 11.0F, false, geom, dtFormat.parse("20130101 12:00:00")),
      TestAttributes("bob",     30, 3, 6.0, 12.0F, false, geom, dtFormat.parse("20120101 12:00:00")),
      TestAttributes("charles", 40, 4, 7.0, 12.0F, false, geom, dtFormat.parse("20120101 12:30:00")))
  .foreach { entry =>
    val feature = builder.buildFeature(entry.name)
    feature.setDefaultGeometry(entry.geom)
    feature.setAttribute("name", entry.name)
    feature.setAttribute("age", entry.age)
    feature.setAttribute("count", entry.count)
    feature.setAttribute("weight", entry.weight)
    feature.setAttribute("height", entry.height)
    feature.setAttribute("admin", entry.admin)
    feature.setAttribute("dtg", entry.dtg)
    // make sure we ask the system to re-use the provided feature-ID
    feature.getUserData().asScala(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    featureCollection.add(feature)
  }

  // write the feature to the store
  val fs = ds.getFeatureSource(sftName).asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]
  fs.addFeatures(featureCollection)

  val indexSchema = IndexSchema(ds.getIndexSchemaFmt(sftName), sft, ds.getFeatureEncoder(sftName))
  val queryPlanner = indexSchema.planner

  "AttributeIndexStrategy" should {
    "print values" in {
      skipped("used for debugging")
      val scanner = connector.createScanner(ds.getAttrIdxTableName(sftName), new Authorizations())
      scanner.setRange(AccRange.prefix("weight"))
      scanner.asScala.foreach(println)
      success
    }
  }

  "AttributeIndexEqualsStrategy" should {

    val strategy = new AttributeIdxEqualsStrategy

    "correctly query on ints" in {
      val query = new Query(sftName, CQL.toFilter("age=21"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList

      features.size mustEqual(1)
      features(0).getAttribute("name") mustEqual("bill")
    }

    "correctly query on longs" in {
      val query = new Query(sftName, CQL.toFilter("count=2"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList

      features.size mustEqual(1)
      features(0).getAttribute("name") mustEqual("bill")
    }

    "correctly query on floats" in {
      val query = new Query(sftName, CQL.toFilter("height=12.0"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList.sortBy(_.getAttribute("name").toString)

      features.size mustEqual(2)
      features(0).getAttribute("name") mustEqual("bob")
      features(1).getAttribute("name") mustEqual("charles")
    }

    "correctly query on floats in different precisions" in {
      val query = new Query(sftName, CQL.toFilter("height=10"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList.sortBy(_.getAttribute("name").toString)

      features.size mustEqual(1)
      features(0).getAttribute("name") mustEqual("alice")
    }

    "correctly query on doubles" in {
      val query = new Query(sftName, CQL.toFilter("weight=6.0"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList.sortBy(_.getAttribute("name").toString)

      features.size mustEqual(2)
      features(0).getAttribute("name") mustEqual("bill")
      features(1).getAttribute("name") mustEqual("bob")
    }

    "correctly query on doubles in different precisions" in {
      val query = new Query(sftName, CQL.toFilter("weight=6"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList.sortBy(_.getAttribute("name").toString)

      features.size mustEqual(2)
      features(0).getAttribute("name") mustEqual("bill")
      features(1).getAttribute("name") mustEqual("bob")
    }

    "correctly query on booleans" in {
      val query = new Query(sftName, CQL.toFilter("admin=false"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList.sortBy(_.getAttribute("name").toString)

      features.size mustEqual(3)
      features(0).getAttribute("name") mustEqual("bill")
      features(1).getAttribute("name") mustEqual("bob")
      features(2).getAttribute("name") mustEqual("charles")
    }

    "correctly query on strings" in {
      val query = new Query(sftName, CQL.toFilter("name='bill'"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList

      features.size mustEqual(1)
      features(0).getAttribute("name") mustEqual("bill")
    }

    "correctly query on date objects" in {
      val query = new Query(sftName, CQL.toFilter("dtg TEQUALS 2012-01-01T12:30:00.000Z"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList

      features.size mustEqual(1)
      features(0).getAttribute("name") mustEqual("charles")
    }

    "correctly query on date strings in standard format" in {
      val query = new Query(sftName, CQL.toFilter("dtg = '2012-01-01T12:30:00.000Z'"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)
      val features = queryPlanner.adaptIterator(results, query).toList

      features.size mustEqual(1)
      features(0).getAttribute("name") mustEqual("charles")
    }
  }

  "AttributeIndexLikeStrategy" should {

    val strategy = new AttributeIdxLikeStrategy

    "correctly query on strings" in {
      val query = new Query(sftName, CQL.toFilter("name LIKE 'b%'"))

      val results = strategy.execute(ds, queryPlanner, sft, query, ExplainNull)

      val features = queryPlanner.adaptIterator(results, query).toList.sortBy(_.getAttribute("name").toString)

      features.size mustEqual(2)
      features(0).getAttribute("name") mustEqual("bill")
      features(1).getAttribute("name") mustEqual("bob")
    }
  }
}
