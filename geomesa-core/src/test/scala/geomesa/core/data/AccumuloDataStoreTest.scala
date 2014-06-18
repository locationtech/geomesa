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

import collection.JavaConversions._
import com.vividsolutions.jts.geom.Coordinate
import geomesa.core.security.{FilteringAuthorizationsProvider, AuthorizationsProvider, DefaultAuthorizationsProvider}
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{Query, DataUtilities, Transaction, DataStoreFinder}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.process.vector.TransformProcess
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreTest extends Specification {

  sequential

  val geotimeAttributes = geomesa.core.index.spec

  var id = 0

  def createStore: AccumuloDataStore = {
    // need to add a unique ID, otherwise create schema will throw an exception
    id = id + 1
    // the specific parameter values should not matter, as we
    // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"       -> "myuser",
      "password"   -> "mypassword",
      "auths"      -> "A,B,C",
      "tableName"  -> ("testwrite" + id),
      "useMock"    -> "true",
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
  }

  "AccumuloDataStore" should {
    "be accessible through DataStoreFinder" in {
      val ds = createStore
      ds should not be null
    }
  }

  "AccumuloDataStore" should {
    "provide ability to create a new store" in {
      val ds = createStore
      val sft = DataUtilities.createType("testType",
        s"NAME:String,$geotimeAttributes")
      ds.createSchema(sft)
      val tx = Transaction.AUTO_COMMIT
      val fw = ds.getFeatureWriterAppend("testType", tx)
      val liveFeature = fw.next()
      liveFeature.setDefaultGeometry(WKTUtils.read("POINT(45.0 49.0)"))
      fw.write()
      tx.commit()
    }
  }

  "AccumuloDataStore" should {
    "provide ability to write using the feature source and read what it wrote" in {
      // create the data store
      val ds = createStore
      val sftName = "testType"
      val sft = DataUtilities.createType(sftName,
        s"NAME:String,$geotimeAttributes")
      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val liveFeature = SimpleFeatureBuilder.build(sft, List(), "fid-1")
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      liveFeature.setDefaultGeometry(geom)

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      featureCollection.add(liveFeature)

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // compose a CQL query that uses a reasonably-sized polygon for searching
      val cqlFilter = CQL.toFilter(s"BBOX(geom, 44.9,48.9,45.1,49.1)")
      val query = new Query(sftName, cqlFilter)

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features
      var containsGeometry = false

      while(features.hasNext) {
        containsGeometry = containsGeometry | features.next.getDefaultGeometry.equals(geom)
      }

      results.getSchema should be equalTo(sft)
      containsGeometry should be equalTo(true)
      res.length should be equalTo(1)
    }

    "return an empty iterator correctly" in {
      // create the data store
      val ds = createStore
      val sftName = "testType"
      val sft = DataUtilities.createType(sftName, s"NAME:String,$geotimeAttributes")
      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      val liveFeature = SimpleFeatureBuilder.build(sft, List("testType", geom, null), "fid-1")
      liveFeature.setDefaultGeometry(geom)

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      featureCollection.add(liveFeature)

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val cqlFilter = CQL.toFilter(s"BBOX(geom, 64.9,68.9,65.1,69.1)")
      val query = new Query(sftName, cqlFilter)

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features
      results.getSchema should be equalTo(sft)
      res.length should be equalTo(1)
      features.hasNext should be equalTo(false)
    }

    "process a DWithin query correctly" in {
      // create the data store
      val ds = createStore
      val sftName = "dwithintest"
      val sft = DataUtilities.createType(sftName, s"NAME:String,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      val liveFeature = SimpleFeatureBuilder.build(sft, List("testType", null, geom), "fid-1")
      liveFeature.setDefaultGeometry(geom)

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      val featureCollection = new DefaultFeatureCollection(sftName, sft)
      featureCollection.add(liveFeature)
      val res = fs.addFeatures(featureCollection)

      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val ff = CommonFactoryFinder.getFilterFactory2
      val geomFactory = JTSFactoryFinder.getGeometryFactory
      val q = ff.dwithin(ff.property("geom"), ff.literal(geomFactory.createPoint(new Coordinate(45.000001, 48.99999))), 100.0, "meters")
      val query = new Query(sftName, q)

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features
      val f = features.next()
      f.getID mustEqual "fid-1"
      features.hasNext must beFalse
    }

    "handle transformations" in {
      // create the data store
      val ds = createStore
      val sftName = "transformtest"
      val sft = DataUtilities.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      val liveFeature = SimpleFeatureBuilder.build(sft, List("testType", null, geom), "fid-1")
      liveFeature.setDefaultGeometry(geom)

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      val featureCollection = new DefaultFeatureCollection(sftName, sft)
      featureCollection.add(liveFeature)
      fs.addFeatures(featureCollection)

      val query = new Query("transformtest", Filter.INCLUDE,
        Array("name", "derived=strConcat('hello',name)", "geom"))

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features
      val f = features.next()

      "name:String,geom:Point:srid=4326,derived:String" mustEqual DataUtilities.encodeType(results.getSchema)
      "fid-1=testType|POINT (45 49)|hellotestType" mustEqual DataUtilities.encodeFeature(f)
    }

    "handle transformations across multiple fields" in {
      // create the data store
      val ds = createStore
      val sftName = "transformtest"
      val sft = DataUtilities.createType(sftName, s"name:String,attr:String,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      val liveFeature = SimpleFeatureBuilder.build(sft, List("testType", "v1", null, geom), "fid-1")
      liveFeature.setDefaultGeometry(geom)

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      val featureCollection = new DefaultFeatureCollection(sftName, sft)
      featureCollection.add(liveFeature)
      fs.addFeatures(featureCollection)

      val query = new Query("transformtest", Filter.INCLUDE,
        Array("name", "derived=strConcat(attr,name)", "geom"))

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features
      val f = features.next()

      "name:String,geom:Point:srid=4326,derived:String" mustEqual DataUtilities.encodeType(results.getSchema)
      "fid-1=testType|POINT (45 49)|v1testType" mustEqual DataUtilities.encodeFeature(f)
    }

    "handle transformations to subtypes" in {
      // create the data store
      val ds = createStore
      val sftName = "transformtest"
      val sft = DataUtilities.createType(sftName, s"name:String,attr:String,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      val liveFeature = SimpleFeatureBuilder.build(sft, List("testType", "v1", null, geom), "fid-1")
      liveFeature.setDefaultGeometry(geom)

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      val featureCollection = new DefaultFeatureCollection(sftName, sft)
      featureCollection.add(liveFeature)
      fs.addFeatures(featureCollection)

      val query = new Query("transformtest", Filter.INCLUDE,
        Array("name", "geom"))

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features
      val f = features.next()

      "name:String,geom:Point:srid=4326" mustEqual DataUtilities.encodeType(results.getSchema)
      "fid-1=testType|POINT (45 49)" mustEqual DataUtilities.encodeFeature(f)
    }

    "provide ability to configure auth provider by static auths" in {
      // create the data store
      val ds = DataStoreFinder.getDataStore(Map(
                     "instanceId" -> "mycloud",
                     "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
                     "user"       -> "myuser",
                     "password"   -> "mypassword",
                     "auths"      -> "user",
                     "tableName"  -> "testwrite",
                     "useMock"    -> "true",
                     "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
      ds should not be null
      ds.authorizationsProvider should beAnInstanceOf[FilteringAuthorizationsProvider]
      ds.authorizationsProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider should beAnInstanceOf[DefaultAuthorizationsProvider]
      ds.authorizationsProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations should be equalTo new Authorizations("user")
    }

    "provide ability to configure auth provider by comma-delimited static auths" in {
      // create the data store
      val ds = DataStoreFinder.getDataStore(Map(
                                                 "instanceId" -> "mycloud",
                                                 "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
                                                 "user"       -> "myuser",
                                                 "password"   -> "mypassword",
                                                 "auths"      -> "user,admin,test",
                                                 "tableName"  -> "testwrite",
                                                 "useMock"    -> "true",
                                                 "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
      ds should not be null
      ds.authorizationsProvider should beAnInstanceOf[FilteringAuthorizationsProvider]
      ds.authorizationsProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider should beAnInstanceOf[DefaultAuthorizationsProvider]
      ds.authorizationsProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations should be equalTo new Authorizations("user", "admin", "test")
    }

    "fail when auth provider system property does not match an actual class" in {
      System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY, "my.fake.Clas")
      try {
      // create the data store
      DataStoreFinder.getDataStore(Map(
                                       "instanceId" -> "mycloud",
                                       "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
                                       "user"       -> "myuser",
                                       "password"   -> "mypassword",
                                       "auths"      -> "user,admin,test",
                                       "tableName"  -> "testwrite",
                                       "useMock"    -> "true",
                                       "featureEncoding" -> "avro")) should throwA[IllegalArgumentException]
      } finally System.clearProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY)
    }

    "fail when schema does not match metadata" in {
      val sftName = "schematest"
      // slight tweak from default - add '-fr' to name
      val schema = s"%~#s%99#r%${sftName}-fr#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id"
      val ds = DataStoreFinder.getDataStore(Map(
                                                 "instanceId" -> "mycloud",
                                                 "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
                                                 "user"       -> "myuser",
                                                 "password"   -> "mypassword",
                                                 "auths"      -> "A,B,C",
                                                 "tableName"  -> "schematest",
                                                 "useMock"    -> "true",
                                                 "indexSchemaFormat"    -> schema,
                                                 "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]

      val sft = DataUtilities.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      val ds2 = DataStoreFinder.getDataStore(Map(
                                                  "instanceId" -> "mycloud",
                                                  "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
                                                  "user"       -> "myuser",
                                                  "password"   -> "mypassword",
                                                  "auths"      -> "A,B,C",
                                                  "tableName"  -> "schematest",
                                                  "useMock"    -> "true",
                                                  "indexSchemaFormat"    -> "xyz",
                                                  "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]

      ds2.getFeatureReader(sftName) should throwA[RuntimeException]
    }

    "allow custom schema metadata if not specified" in {
      // relies on data store created in previous test
      val ds = DataStoreFinder.getDataStore(Map(
                                                 "instanceId" -> "mycloud",
                                                 "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
                                                 "user"       -> "myuser",
                                                 "password"   -> "mypassword",
                                                 "auths"      -> "A,B,C",
                                                 "tableName"  -> "schematest",
                                                 "useMock"    -> "true",
                                                 "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
      val sftName = "schematest"

      val fr = ds.getFeatureReader(sftName)
      fr should not be null
    }

    "allow users with sufficient auths to write data" in {
      // create the data store
      val ds = DataStoreFinder.getDataStore(Map(
                                                 "instanceId" -> "mycloud",
                                                 "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
                                                 "user"       -> "myuser",
                                                 "password"   -> "mypassword",
                                                 "auths"      -> "user,admin",
                                                 "visibilities" -> "user&admin",
                                                 "tableName"  -> "testwrite",
                                                 "useMock"    -> "true",
                                                 "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
      ds should not be null

      // create the schema - the auths for this user are sufficient to write data
      val sftName = "authwritetest1"
      val sft = DataUtilities.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      // write some data
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      val written = fs.addFeatures(new ListFeatureCollection(sft, getFeatures(sft).toList))

      written should not be null
      written.length mustEqual(6)
    }

    "restrict users with insufficient auths from writing data" in {
      // create the data store
      val ds = DataStoreFinder.getDataStore(Map(
                                                 "instanceId" -> "mycloud",
                                                 "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
                                                 "user"       -> "myuser",
                                                 "password"   -> "mypassword",
                                                 "auths"      -> "user",
                                                 "visibilities" -> "user&admin",
                                                 "tableName"  -> "testwrite",
                                                 "useMock"    -> "true",
                                                 "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
      ds should not be null

      // create the schema - the auths for this user are less than the visibility used to write data
      val sftName = "authwritetest2"
      val sft = DataUtilities.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      // write some data
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      try {
        // this should throw an exception
        fs.addFeatures(new ListFeatureCollection(sft, getFeatures(sft).toList))
        failure("Should not be able to write data")
      } catch {
        case e: RuntimeException => success
      }
    }

  }

  def getFeatures(sft: SimpleFeatureType) = (0 until 6).map { i =>
    val builder = new SimpleFeatureBuilder(sft)
    builder.set("geom", WKTUtils.read("POINT(45.0 45.0)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }

  "AccumuloFeatureStore" should {
    "compute target schemas from transformation expressions" in {
      val origSFT = DataUtilities.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val definitions =
        TransformProcess.toDefinition("name=name;helloName=strConcat('hello', name);geom=geom")

      val result = AccumuloFeatureStore.computeSchema(origSFT, definitions.toSeq)
      println(DataUtilities.encodeType(result))

      (result must not).beNull
    }
  }
}
