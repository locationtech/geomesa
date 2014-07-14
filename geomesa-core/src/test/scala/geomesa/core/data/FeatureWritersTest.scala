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

import java.text.SimpleDateFormat
import java.util.TimeZone

import geomesa.core.index.SF_PROPERTY_START_TIME
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.text.WKTUtils
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FeatureWritersTest extends Specification {

  sequential

  val geotimeAttributes = geomesa.core.index.spec
  val sftName = "mutableType"
  val sft = DataUtilities.createType(sftName, s"name:String,age:Integer,$geotimeAttributes")
  sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
  val sdf = new SimpleDateFormat("yyyyMMdd")
  sdf.setTimeZone(TimeZone.getTimeZone("Zulu"))
  val dateToIndex = sdf.parse("20140102")

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
      "tableName"  -> "differentTableFromOtherTests", //note the table needs to be different to prevent testing errors,
      "useMock"    -> "true")
    ).asInstanceOf[AccumuloDataStore]

    "AccumuloFeatureWriter" should {
      "provide ability to update a single feature that it wrote and preserve feature IDs" in {
        val ds = createStore
        ds.createSchema(sft)
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val featureCollection = new DefaultFeatureCollection(sftName, sft)

        /* create a feature */
        val originalFeature1 = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "id1")
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        originalFeature1.setDefaultGeometry(geom)
        originalFeature1.setAttribute("name","fred")
        originalFeature1.setAttribute("age",50.asInstanceOf[Any])

        /* make sure we ask the system to re-use the provided feature-ID */
        originalFeature1.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(originalFeature1)

        /* create a second feature */
        val originalFeature2 = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "id2")
        originalFeature2.setDefaultGeometry(geom)
        originalFeature2.setAttribute("name","tom")
        originalFeature2.setAttribute("age",60.asInstanceOf[Any])

        /* make sure we ask the system to re-use the provided feature-ID */
        originalFeature2.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(originalFeature2)

        /* create a third feature */
        val originalFeature3 = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "id3")
        originalFeature3.setDefaultGeometry(geom)
        originalFeature3.setAttribute("name","kyle")
        originalFeature3.setAttribute("age",2.asInstanceOf[Any])

        /* make sure we ask the system to re-use the provided feature-ID */
        originalFeature3.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(originalFeature3)

        /* write the feature to the store */
        fs.addFeatures(featureCollection)
        fs.flush()

        val store = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        /* turn fred into billy */
        val filter = CQL.toFilter("name = 'fred'")
        store.modifyFeatures(Array("name", "age"), Array("billy", 25.asInstanceOf[AnyRef]), filter)

        /* delete kyle */
        val deleteFilter = CQL.toFilter("name = 'kyle'");
        store.removeFeatures(deleteFilter)

        /* query everything */
        val cqlFilter = CQL.toFilter("include")

        /* Let's read out what we wrote...we should only get tom and billy back out */
        val nameAgeMap = getMap[String, Int](getFeatures(sftName, fs, "include"), "name", "age")
        nameAgeMap.size should equalTo(2)
        nameAgeMap should contain( "tom" -> 60)
        nameAgeMap should contain( "billy" -> 25)

        val featureIdMap = getMap[String, String](getFeatures(sftName, fs, "include"), "name", (sf:SimpleFeature) => sf.getID)
        featureIdMap.size should equalTo(2)
        featureIdMap should contain( "tom" -> "id2")
        featureIdMap should contain( "billy" -> "id1")
      }

      "be able to replace all features in a store using a general purpose FeatureWriter" in {
        val ds = createStore
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        /* from the test before there are 2 features left over - validate that's true and delete  */
        countFeatures(fs, sftName) should equalTo(2)
        val writer = ds.getFeatureWriter(sftName, Transaction.AUTO_COMMIT)

        while(writer.hasNext){
          writer.next
          writer.remove()
        }

        // cannot do anything here until the writer is closed.

        /* repopulate it */
        val sftType = ds.getSchema(sftName)
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        val c = new DefaultFeatureCollection
        c.add(AvroSimpleFeatureFactory.buildAvroFeature(sftType, Array("will", 56.asInstanceOf[AnyRef], geom, dateToIndex, null), "fid1"))
        c.add(AvroSimpleFeatureFactory.buildAvroFeature(sftType, Array("george", 33.asInstanceOf[AnyRef], geom, dateToIndex, null), "fid2"))
        c.add(AvroSimpleFeatureFactory.buildAvroFeature(sftType, Array("sue", 99.asInstanceOf[AnyRef], geom, dateToIndex, null), "fid3"))
        c.add(AvroSimpleFeatureFactory.buildAvroFeature(sftType, Array("karen", 50.asInstanceOf[AnyRef], geom, dateToIndex, null), "fid4"))
        c.add(AvroSimpleFeatureFactory.buildAvroFeature(sftType, Array("bob", 56.asInstanceOf[AnyRef], geom, dateToIndex, null), "fid5"))

        val ids = c.map { f => f.getID}
        try {
          c.zip(ids).foreach { case (feature, id) =>
            val writerCreatedFeature = writer.next()
            writerCreatedFeature.setAttributes(feature.getAttributes)
            writerCreatedFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writerCreatedFeature.getUserData.put(Hints.PROVIDED_FID, id)
            writer.write()
          }
        } finally { writer.close() }

        countFeatures(fs, sftName) should equalTo(5)

        /* this tests the Hints.PROVIDED_FID feature */
        val featureIdMap = getMap[String, String](getFeatures(sftName, fs, "include"), "name", (sf: SimpleFeature) => sf.getID)
        featureIdMap.size should equalTo(5)
        featureIdMap should contain("will" -> "fid1")
        featureIdMap should contain("george" -> "fid2")
        featureIdMap should contain("sue" -> "fid3")
        featureIdMap should contain("karen" -> "fid4")
        featureIdMap should contain("bob" -> "fid5")
      }

      "be able to update all features based on some ecql or something" in {
        val ds = createStore
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val filter = CQL.toFilter("(age > 50 AND age < 99) or (name = 'karen')");
        fs.modifyFeatures(Array("age"), Array(60.asInstanceOf[AnyRef]), filter)

        val nameAgeMap = getMap[String, Int](getFeatures(sftName, fs, "age = 60"), "name", "age")
        nameAgeMap.size should equalTo(3)
        nameAgeMap should contain( "will" -> 60)
        nameAgeMap should contain( "karen" -> 60)
        nameAgeMap should contain( "bob" -> 60)

        /* feature id should stay the same */
        val featureIdMap = getMap[String, String](getFeatures(sftName, fs, "age = 60"),"name", (sf:SimpleFeature) => sf.getID)
        featureIdMap.size should equalTo(3)
        featureIdMap should contain("will" -> "fid1")
        featureIdMap should contain("karen" -> "fid4")
        featureIdMap should contain("bob" -> "fid5")
      }

      "provide ability to add data inside transactions" in {
        val ds = createStore
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val sftType = ds.getSchema(sftName)
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        val c = new DefaultFeatureCollection
        c.add(AvroSimpleFeatureFactory.buildAvroFeature(sftType, Array("dude1", 15.asInstanceOf[AnyRef], geom, null, null), "fid10"))
        c.add(AvroSimpleFeatureFactory.buildAvroFeature(sftType, Array("dude2", 16.asInstanceOf[AnyRef], geom, null, null), "fid11"))
        c.add(AvroSimpleFeatureFactory.buildAvroFeature(sftType, Array("dude3", 17.asInstanceOf[AnyRef], geom, null, null), "fid12"))

        val trans = new DefaultTransaction("trans1")
        fs.setTransaction(trans)
        try {
          fs.addFeatures(c)
          trans.commit()

          val features = getFeatures(sftName, fs, "(age = 15) or (age = 16) or (age = 17)")
          val nameAgeMap = getMap[String, Int](features, "name", "age")
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
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
        val trans = new DefaultTransaction("trans1")
        fs.setTransaction(trans)
        try {
          fs.removeFeatures(CQL.toFilter("name = 'dude1' or name='dude2' or name='dude3'"))
          trans.commit()

          val nameAgeMap = getMap[String, Int](getFeatures(sftName, fs, "include"), "name", "age")
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

      "issue delete keys when geometry changes" in {
        val ds = createStore
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val featureCollection = new DefaultFeatureCollection(sftName, sft)
        val filter = CQL.toFilter("name = 'bob' or name='karen'")
        val writer = ds.getFeatureWriter(sftName, filter, Transaction.AUTO_COMMIT)

        while(writer.hasNext){
          val sf = writer.next
          sf.setDefaultGeometry(WKTUtils.read("POINT(50.0 50)"))
          writer.write
        }
        writer.close

        // Verify old geo bbox doesn't return them
        val map45 = getMap[String,Int](getFeatures(sftName, fs, "BBOX(geom, 44.9,48.9,45.1,49.1)"),"name", "age")
        map45.keySet.size should equalTo(3)
        map45.keySet should containAllOf(List("will", "george", "sue"))

        // Verify that new geometries are written with a bbox query that uses the index
        val map50 = getMap[String,Int](getFeatures(sftName, fs, "BBOX(geom, 49.9,49.9,50.1,50.1)"),"name", "age")
        map50.keySet.size should equalTo(2)
        map50.keySet should containAllOf(List("bob", "karen"))

        // get them all
        val mapLarge = getMap[String,Int](getFeatures(sftName, fs, "BBOX(geom, 44.0,44.0,51.0,51.0)"),"name", "age")
        mapLarge.keySet.size should equalTo(5)
        mapLarge.keySet should containAllOf(List("will", "george", "sue", "bob", "karen"))

        // get none
        val mapNone = getMap[String,Int](getFeatures(sftName, fs, "BBOX(geom, 30.0,30.0,31.0,31.0)"),"name", "age")
        mapNone.keySet.size should equalTo(0)
      }

      "issue delete keys when datetime changes" in {
        val ds = createStore
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val attr = "dtg"

        val filter = CQL.toFilter("name = 'will' or name='george'")
        val writer = ds.getFeatureWriter(sftName, filter, Transaction.AUTO_COMMIT)

        val newDate = sdf.parse("20140202")
        while(writer.hasNext){
          val sf = writer.next
          sf.setAttribute(attr, newDate)
          writer.write
        }
        writer.close

        // Verify old daterange doesn't return them
        val mapJan = getMap[String,Int](getFeatures(sftName, fs, s"$attr DURING 2013-12-29T00:00:00Z/2014-01-04T00:00:00Z"),"name", "age")
        mapJan.keySet.size should equalTo(3)
        mapJan.keySet should containAllOf(List("sue", "bob", "karen"))

        // Verify new date range returns things
        val mapFeb = getMap[String,Int](getFeatures(sftName, fs, s"$attr DURING 2014-02-01T00:00:00Z/2014-02-03T00:00:00Z"),"name", "age")
        mapFeb.keySet.size should equalTo(2)
        mapFeb.keySet should containAllOf(List("will","george"))

        // Verify large date range returns everything
        val mapJanFeb = getMap[String,Int](getFeatures(sftName, fs, s"$attr DURING 2014-01-01T00:00:00Z/2014-02-03T00:00:00Z"),"name", "age")
        mapJanFeb.keySet.size should equalTo(5)
        mapJanFeb.keySet should containAllOf(List("will", "george", "sue", "bob", "karen"))

        // Verify other date range returns nothing
        val map2013 = getMap[String,Int](getFeatures(sftName, fs, s"$attr DURING 2013-01-01T00:00:00Z/2013-12-31T00:00:00Z"),"name", "age")
        map2013.keySet.size should equalTo(0)
      }

      "ensure that feature IDs are not changed when spatiotemporal indexes change" in {
        val ds = createStore
        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        val origFeatures =  {
          val features = getFeatures(sftName, fs, "include")
          val map = collection.mutable.HashMap.empty[String,SimpleFeature]
          while(features.hasNext) {
            val sf = features.next()
            map.put(sf.getAttribute("name").asInstanceOf[String], sf)
          }
          map.toMap
        }
        origFeatures.size should be equalTo(5)

        val filter = CQL.toFilter("include")
        val writer = ds.getFeatureWriter(sftName, filter, Transaction.AUTO_COMMIT)
        val attr = "dtg"
        val newDate = sdf.parse("20120102")
        while(writer.hasNext){
          val sf = writer.next
          sf.setAttribute(attr, newDate)
          sf.setDefaultGeometry(WKTUtils.read("POINT(10.0 10.0)"))
          writer.write
        }
        writer.close

        val newFeatures =  {
          val features = getFeatures(sftName, fs, "include")
          val map = collection.mutable.HashMap.empty[String,SimpleFeature]
          while(features.hasNext) {
            val sf = features.next()
            map.put(sf.getAttribute("name").asInstanceOf[String], sf)
          }
          map.toMap
        }
        newFeatures.size should be equalTo(origFeatures.size)

        newFeatures.keys.foreach { k =>
          val o = origFeatures(k)
          val n = newFeatures(k)

          o.getID must be equalTo(n.getID)
          o.getDefaultGeometry must not be equalTo(n.getDefaultGeometry)
          o.getAttribute(attr) must not be equalTo(n.getAttribute(attr))
        }
      }

    }

  def getFeatures(sftName: String, store: AccumuloFeatureStore, cql: String): SimpleFeatureIterator = {
    val query = new Query(sftName, CQL.toFilter(cql))
    val results = store.getFeatures(query)
    results.features
  }

  def getMap[K,V](features: SimpleFeatureIterator, keyAttr: String, valAttr: String) : Map[K, V] = {
    getMap[K,V](features, keyAttr, (sf:SimpleFeature) => sf.getAttribute(valAttr))
  }

  def getMap[K,V](features: SimpleFeatureIterator, keyAttr: String, valFunc: (SimpleFeature => AnyRef)) : Map[K, V] = {
    val map = collection.mutable.HashMap.empty[K, V]
    while(features.hasNext) {
      val sf = features.next()
      map.put(sf.getAttribute(keyAttr).asInstanceOf[K], valFunc(sf).asInstanceOf[V])
    }
    map.toMap
  }

  def countFeatures(store:AccumuloFeatureStore, sftName:String): Int = {
    val features = getFeatures(sftName, store, "include")
    var count = 0
    while(features.hasNext) { features.next(); count += 1}
    count
  }

}
