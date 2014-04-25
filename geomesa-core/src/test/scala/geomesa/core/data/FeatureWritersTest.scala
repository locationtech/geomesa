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

package geomesa.core.data

import scala.collection.JavaConversions._
import geomesa.utils.text.WKTUtils
import org.geotools.data._
import org.geotools.factory.{GeoTools, CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.collection

@RunWith(classOf[JUnitRunner])
class FeatureWritersTest extends Specification {

  sequential

  val geotimeAttributes = geomesa.core.index.spec

  def createStore: AccumuloDataStore =
    // the specific parameter values should not matter, as we
    // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(
      Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"       -> "myuser",
      "password"   -> "mypassword",
      "auths"      -> "A,B,C",
      "tableName"  -> "testwrite",
      "useMock"    -> "true",
      "featureEncoding" -> "avro")
    ).asInstanceOf[AccumuloDataStore]

    "AccumuloDataStore" should {
      "provide ability to update a single feature that it wrote and preserve feature IDs" in {
        val ds = createStore
        val sftName = "mutableType"
        val sft = DataUtilities.createType(sftName, s"name:String,age:Integer,$geotimeAttributes")
        ds.createSchema(sft)
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val featureCollection = new DefaultFeatureCollection(sftName, sft)

        /* create a feature */
        val originalFeature1 = SimpleFeatureBuilder.build(sft, List(), "id1")
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        originalFeature1.setDefaultGeometry(geom)
        originalFeature1.setAttribute("name","fred")
        originalFeature1.setAttribute("age",50.asInstanceOf[Any])

        /* make sure we ask the system to re-use the provided feature-ID */
        originalFeature1.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(originalFeature1)

        /* create a second feature */
        val originalFeature2 = SimpleFeatureBuilder.build(sft, List(), "id2")
        originalFeature2.setDefaultGeometry(geom)
        originalFeature2.setAttribute("name","tom")
        originalFeature2.setAttribute("age",60.asInstanceOf[Any])

        /* make sure we ask the system to re-use the provided feature-ID */
        originalFeature2.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(originalFeature2)

        /* create a second feature */
        val originalFeature3 = SimpleFeatureBuilder.build(sft, List(), "id3")
        originalFeature3.setDefaultGeometry(geom)
        originalFeature3.setAttribute("name","kyle")
        originalFeature3.setAttribute("age",2.asInstanceOf[Any])

        /* make sure we ask the system to re-use the provided feature-ID */
        originalFeature3.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(originalFeature3)

        /* write the feature to the store */
        fs.addFeatures(featureCollection)

        val store = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        /* turn fred into billy */
        val filter = CQL.toFilter("name = 'fred'");
        store.modifyFeatures(Array("name", "age"), Array("billy", 25.asInstanceOf[AnyRef]), filter)

        /* delete kyle */
        val deleteFilter = CQL.toFilter("name = 'kyle'");
        store.removeFeatures(deleteFilter)

        /* query everything */
        val cqlFilter = CQL.toFilter("include")
        val query = new Query(sftName, cqlFilter)

        /* Let's read out what we wrote...we should only get tom and billy back out */
        val results = fs.getFeatures(query)
        val features = results.features
        val nameAgeMap = collection.mutable.HashMap.empty[String, Int]
        val featureIdMap = collection.mutable.HashMap.empty[String, String]
        while(features.hasNext) {
          val sf = features.next()
          nameAgeMap.put(sf.getAttribute("name").asInstanceOf[String], sf.getAttribute("age").asInstanceOf[Int])
          featureIdMap.put(sf.getAttribute("name").asInstanceOf[String], sf.getID)
        }

        nameAgeMap.size should equalTo(2)
        nameAgeMap should contain( "tom" -> 60)
        nameAgeMap should contain( "billy" -> 25)

        featureIdMap.size should equalTo(2)
        featureIdMap should contain( "tom" -> "id2")
        featureIdMap should contain( "billy" -> "id1")
      }

      "be able to replace all features in a store using a general purpose FeatureWriter" in {
        val ds = createStore
        val sftName = "mutableType"
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        /* from the test before there are 2 features left over - validate that's true and delete  */
        countFeatures(fs, sftName) should equalTo(2)
        val writer = ds.getFeatureWriter(sftName, Transaction.AUTO_COMMIT)

        while(writer.hasNext){
          writer.next
          writer.remove
        }

        // cannot do anything here until the writer is closed.

        /* repopulate it */
        val sftType = ds.getSchema(sftName)
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        val c = new DefaultFeatureCollection
        c.add(SimpleFeatureBuilder.build(sftType, Array("will", 56.asInstanceOf[AnyRef], geom, null, null), "fid1"))
        c.add(SimpleFeatureBuilder.build(sftType, Array("george", 33.asInstanceOf[AnyRef], geom, null, null), "fid2"))
        c.add(SimpleFeatureBuilder.build(sftType, Array("sue", 99.asInstanceOf[AnyRef], geom, null, null), "fid3"))
        c.add(SimpleFeatureBuilder.build(sftType, Array("karen", 50.asInstanceOf[AnyRef], geom, null, null), "fid4"))
        c.add(SimpleFeatureBuilder.build(sftType, Array("bob", 56.asInstanceOf[AnyRef], geom, null, null), "fid5"))

        val ids = c.map { f => f.getID}
        try {
          c.zip(ids).foreach { case (feature, id) =>
            val writerCreatedFeature = writer.next()
            writerCreatedFeature.setAttributes(feature.getAttributes)
            writerCreatedFeature.getUserData()(Hints.PROVIDED_FID) = id
            writer.write
          }
        } finally { writer.close }

        countFeatures(fs, sftName) should equalTo(5)

        val query = new Query(sftName, CQL.toFilter("include"))

        val results = fs.getFeatures(query)
        val features = results.features
        val featureIdMap = collection.mutable.HashMap.empty[String, String]
        while(features.hasNext) {
          val sf = features.next()
          featureIdMap.put(sf.getAttribute("name").asInstanceOf[String], sf.getID)
        }

        /* this tests the Hints.PROVIDED_FID feature */

        featureIdMap.size should equalTo(5)
        featureIdMap should contain("will" -> "fid1")
        featureIdMap should contain("george" -> "fid2")
        featureIdMap should contain("sue" -> "fid3")
        featureIdMap should contain("karen" -> "fid4")
        featureIdMap should contain("bob" -> "fid5")
      }

      "be able to update all features based on some ecql or something" in {
        val ds = createStore
        val sftName = "mutableType"
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val filter = CQL.toFilter("(age > 50 AND age < 99) or (name = 'karen')");
        fs.modifyFeatures(Array("age"), Array(60.asInstanceOf[AnyRef]), filter)

        val query = new Query(sftName, CQL.toFilter("age = 60"))

        val results = fs.getFeatures(query)
        val features = results.features
        val nameAgeMap = collection.mutable.HashMap.empty[String, Int]
        val featureIdMap = collection.mutable.HashMap.empty[String, String]
        while(features.hasNext) {
          val sf = features.next()
          nameAgeMap.put(sf.getAttribute("name").asInstanceOf[String], sf.getAttribute("age").asInstanceOf[Int])
          featureIdMap.put(sf.getAttribute("name").asInstanceOf[String], sf.getID)
        }

        nameAgeMap.size should equalTo(3)
        nameAgeMap should contain( "will" -> 60)
        nameAgeMap should contain( "karen" -> 60)
        nameAgeMap should contain( "bob" -> 60)

        /* feature id should stay the same */
        featureIdMap.size should equalTo(3)
        featureIdMap should contain("will" -> "fid1")
        featureIdMap should contain("karen" -> "fid4")
        featureIdMap should contain("bob" -> "fid5")
      }

      "provide ability to add data inside transactions" in {
        val ds = createStore
        val sftName = "mutableType"
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val sftType = ds.getSchema(sftName)
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        val c = new DefaultFeatureCollection
        c.add(SimpleFeatureBuilder.build(sftType, Array("dude1", 15.asInstanceOf[AnyRef], geom, null, null), "fid10"))
        c.add(SimpleFeatureBuilder.build(sftType, Array("dude2", 16.asInstanceOf[AnyRef], geom, null, null), "fid11"))
        c.add(SimpleFeatureBuilder.build(sftType, Array("dude3", 17.asInstanceOf[AnyRef], geom, null, null), "fid12"))

        val trans = new DefaultTransaction("trans1")
        fs.setTransaction(trans)
        try {
          fs.addFeatures(c)
          trans.commit()

          val query = new Query(sftName, CQL.toFilter("(age = 15) or (age = 16) or (age = 17)"))
          val results = fs.getFeatures(query)
          val features = results.features
          val nameAgeMap = collection.mutable.HashMap.empty[String, Int]
          while(features.hasNext) {
            val sf = features.next()
            nameAgeMap.put(sf.getAttribute("name").asInstanceOf[String], sf.getAttribute("age").asInstanceOf[Int])
          }

          nameAgeMap.size should equalTo(3)
          nameAgeMap should contain( "dude1" -> 15)
          nameAgeMap should contain( "dude2" -> 16)
          nameAgeMap should contain( "dude3" -> 17)

        } catch {
          case e: Exception => {
            trans.rollback()
            throw e
          }
        } finally {
          trans.close()
        }
      }

      "provide ability to remove inside transactions" in {
        val ds = createStore
        val sftName = "mutableType"
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
        val trans = new DefaultTransaction("trans1")
        fs.setTransaction(trans)
        try {
          fs.removeFeatures(CQL.toFilter("name = 'dude1' or name='dude2' or name='dude3'"))
          trans.commit()

          val query = new Query(sftName, CQL.toFilter("include"))
          val results = fs.getFeatures(query)
          val features = results.features
          val nameAgeMap = collection.mutable.HashMap.empty[String, Int]
          while(features.hasNext) {
            val sf = features.next()
            nameAgeMap.put(sf.getAttribute("name").asInstanceOf[String], sf.getAttribute("age").asInstanceOf[Int])
          }

          nameAgeMap.keySet should not contain ("dude1")
          nameAgeMap.keySet should not contain ("dude2")
          nameAgeMap.keySet should not contain ("dude3")

          nameAgeMap.keySet should containAllOf(List("will", "george", "sue", "karen", "bob"))
          nameAgeMap.size should equalTo(5)

        } catch {
          case e: Exception => {
            trans.rollback()
            throw e
          }
        }
        finally {
          trans.close()
        }
      }
    }

  def countFeatures(fs:AccumuloFeatureStore, sftName:String): Int = {
    val cqlFilter = CQL.toFilter("include")
    val query = new Query(sftName, cqlFilter)
    val results = fs.getFeatures(query)
    val features = results.features
    var count = 0
    while(features.hasNext) { features.next(); count = count + 1}
    count
  }

}
