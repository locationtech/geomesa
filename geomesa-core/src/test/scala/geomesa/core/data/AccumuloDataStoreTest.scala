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

import com.vividsolutions.jts.geom.{PrecisionModel, Point, Coordinate}
import geomesa.core.index.SF_PROPERTY_START_TIME
import geomesa.core.security.{AuthorizationsProvider, DefaultAuthorizationsProvider, FilteringAuthorizationsProvider}
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.codec.binary.Hex
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.process.vector.TransformProcess
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreTest extends Specification {

  sequential

  val geotimeAttributes = geomesa.core.index.spec
  var id = 0
  val hints = new Hints(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)
  val gf = JTSFactoryFinder.getGeometryFactory

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
    "create a data store" >> {
      val ds = createStore
      "that is not null" >> { ds must not be null }
      "and create a schema" >> {
        val sftName = "testType"
        val sft = SimpleFeatureTypes.createType(sftName, s"NAME:String,$geotimeAttributes")
        sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
        ds.createSchema(sft)

        "getSchema must return null when name exists" >> {
          ds.getSchema(sftName) must not be null
        }

        "getTypeNames should contain newly created type" >> { ds.getTypeNames.toSeq must contain("testType") }

        "provide ability to write using the feature source and read what it wrote" >> {
          val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]

          // create a feature
          val builder = new SimpleFeatureBuilder(sft, featureFactory)
          val liveFeature = builder.buildFeature("fid-1")
          val geom = WKTUtils.read("POINT(45.0 49.0)")
          liveFeature.setDefaultGeometry(geom)

          // make sure we ask the system to re-use the provided feature-ID
          liveFeature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

          val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)

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

          "results schema should match" >> { results.getSchema should be equalTo sft }
          "geometry should be set" >> { containsGeometry should be equalTo true }
          "result length should be 1" >> { res.length should be equalTo 1 }
        }

        "return NULL when a feature name does not exist" in {
          val sftName = "testTypeThatDoesNotExist"
          ds.getSchema(sftName) must beNull
        }

        "return an empty iterator correctly" in {
          // create the data store
          val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

          // create a feature
          val geom = WKTUtils.read("POINT(45.0 49.0)")
          val builder = new SimpleFeatureBuilder(sft, featureFactory)
          builder.addAll(List("testType", geom, null))
          val liveFeature = builder.buildFeature("fid-1")

          // make sure we ask the system to re-use the provided feature-ID
          liveFeature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

          val featureCollection = new DefaultFeatureCollection(sftName, sft)

          featureCollection.add(liveFeature)

          // write the feature to the store
          val res = fs.addFeatures(featureCollection)
          "after writing 1 feature" >> { res.length should be equalTo 1 }

          // compose a CQL query that uses a polygon that is disjoint with the feature bounds
          val cqlFilter = CQL.toFilter(s"BBOX(geom, 64.9,68.9,65.1,69.1)")
          val query = new Query(sftName, cqlFilter)

          // Let's read out what we wrote.
          val results = fs.getFeatures(query)
          val features = results.features

          "where schema matches" >> { results.getSchema should be equalTo sft }
          "and there are no results" >> { features.hasNext should be equalTo false }
        }
      }
      "process a DWithin query correctly" in {
        // create the data store
        val sftName = "dwithintest"
        val sft = SimpleFeatureTypes.createType(sftName, s"NAME:String,dtg:Date,*geom:Point:srid=4326")
        sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
        ds.createSchema(sft)

        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        // create a feature
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        val builder = new SimpleFeatureBuilder(sft, featureFactory)
        builder.addAll(List("testType", null, geom))
        val liveFeature = builder.buildFeature("fid-1")

        // make sure we ask the system to re-use the provided feature-ID
        liveFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        val featureCollection = new DefaultFeatureCollection(sftName, sft)
        featureCollection.add(liveFeature)
        fs.addFeatures(featureCollection)

        // compose a CQL query that uses a polygon that is disjoint with the feature bounds
        val ff = CommonFactoryFinder.getFilterFactory2
        val geomFactory = JTSFactoryFinder.getGeometryFactory
        val q = ff.dwithin(ff.property("geom"), ff.literal(geomFactory.createPoint(new Coordinate(45.000001, 48.99999))), 100.0, "meters")
        val query = new Query(sftName, q)

        // Let's read out what we wrote.
        val results = fs.getFeatures(query)
        val features = results.features
        val f = features.next()

        "with correct result" >> { f.getID mustEqual "fid-1" }
        "and no more results" >> { features.hasNext must beFalse }
      }

      "handle transformations" in {
        val sftName = "transformtest1"
        val sft = SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
        sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
        ds.createSchema(sft)

        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        // create a feature
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        val builder = new SimpleFeatureBuilder(sft, featureFactory)
        builder.addAll(List("testType", null, geom))
        val liveFeature = builder.buildFeature("fid-1")

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

        "with matching schema" >> {
          "name:String:index=false,*geom:Point:srid=4326:index=false,derived:String:index=false" mustEqual
            SimpleFeatureTypes.encodeType(results.getSchema)
        }

        "and correct result" >> { "fid-1=testType|POINT (45 49)|hellotestType" mustEqual DataUtilities.encodeFeature(f) }
      }

      "handle transformations across multiple fields" in {
        // create the data store
        val sftName = "transformtest2"
        val sft = SimpleFeatureTypes.createType(sftName, s"name:String,attr:String,dtg:Date,*geom:Point:srid=4326")
        sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
        ds.createSchema(sft)

        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        // create a feature
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        val builder = new SimpleFeatureBuilder(sft, featureFactory)
        builder.addAll(List("testType", "v1", null, geom))
        val liveFeature = builder.buildFeature("fid-1")

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

        "with matching schemas" >> {
          "name:String:index=false,*geom:Point:srid=4326:index=false,derived:String:index=false" mustEqual SimpleFeatureTypes.encodeType(results.getSchema)
        }

        "and correct results" >> {
          "fid-1=testType|POINT (45 49)|v1testType" mustEqual DataUtilities.encodeFeature(f)
        }
      }

      "handle transformations to subtypes" in {
        // create the data store
        val sftName = "transformtest3"
        val sft = SimpleFeatureTypes.createType(sftName, s"name:String,attr:String,dtg:Date,*geom:Point:srid=4326")
        sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
        ds.createSchema(sft)

        val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

        // create a feature
        val geom = WKTUtils.read("POINT(45.0 49.0)")
        val builder = new SimpleFeatureBuilder(sft, featureFactory)
        builder.addAll(List("testType", "v1", null, geom))
        val liveFeature = builder.buildFeature("fid-1")

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

        "with matching schemas" >> {
          "name:String:index=false,*geom:Point:srid=4326:index=false" mustEqual SimpleFeatureTypes.encodeType(results.getSchema)
        }

        "and correct results" >> {
          "fid-1=testType|POINT (45 49)" mustEqual DataUtilities.encodeFeature(f)
        }
      }
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

      val sft = SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
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
      val sft = SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
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
      val sft = SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
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

    "allow secondary attribute indexes" >> {
      val table = "testing_secondary_index"
      val ds = DataStoreFinder.getDataStore(Map(
        "instanceId" -> "mycloud",
        "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"       -> "myuser",
        "password"   -> "mypassword",
        "tableName"  -> table,
        "useMock"    -> "true")).asInstanceOf[AccumuloDataStore]

      // accumulo supports only alphanum + underscore aka ^\\w+$
      // this should be OK
      val sftName = "somethingsaf3"
      val sft = SimpleFeatureTypes.createType(sftName, s"name:String:index=true,numattr:Integer:index=false,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      val mockInstance = new MockInstance("mycloud")
      val c = mockInstance.getConnector("myuser", new PasswordToken("mypassword".getBytes("UTF8")))

      "create all appropriate tables" >> {
        "catalog table" >> { c.tableOperations().exists(table) must beTrue }
        "st_idx table" >> { c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beTrue }
        "records table" >> { c.tableOperations().exists(s"${table}_${sftName}_records") must beTrue }
        "attr idx table" >> { c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beTrue }
      }

      val pt = gf.createPoint(new Coordinate(0, 0))
      val one = AvroSimpleFeatureFactory.buildAvroFeature(sft, Seq("one", new Integer(1), new DateTime(), pt), "1")
      val two = AvroSimpleFeatureFactory.buildAvroFeature(sft, Seq("two", new Integer(2), new DateTime(), pt), "2")

      val fs = ds.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]
      fs.addFeatures(DataUtilities.collection(List(one, two)))
      fs.flush()

      val ff = CommonFactoryFinder.getFilterFactory2

      "query indexed attribute" >> {
        val q1 = ff.equals(ff.property("name"), ff.literal("one"))
        val fr = ds.getFeatureReader(sftName, new Query(sftName, q1))
        val results = fr.iter.toList
        results must haveLength(1)
        results.head.getAttribute("name") must beEqualTo("one")
      }

      "query non-indexed attributes" >> {
        val q2 = ff.equals(ff.property("numattr"), ff.literal(2))
        val fr = ds.getFeatureReader(sftName, new Query(sftName, q2))
        val results = fr.iter.toList
        results must haveLength(1)
        results.head.getAttribute("numattr") must beEqualTo(2)
      }
    }

    "hex encode multibyte chars as multiple underscore + hex" in {
      val table = "testing_chinese_features"
      val ds = DataStoreFinder.getDataStore(Map(
        "instanceId" -> "mycloud",
        "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"       -> "myuser",
        "password"   -> "mypassword",
        "tableName"  -> table,
        "useMock"    -> "true")).asInstanceOf[AccumuloDataStore]

      ds should not be null

      // accumulo supports only alphanum + underscore aka ^\\w+$
      // this should end up hex encoded
      val sftName = "nihao你好"
      val sft = SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      val mockInstance = new MockInstance("mycloud")
      val c = mockInstance.getConnector("myuser", new PasswordToken("mypassword".getBytes("UTF8")))

      // encode groups of 2 hex chars since we are doing multibyte chars
      def enc(s: String): String = Hex.encodeHex(s.getBytes("UTF8")).grouped(2)
        .map{ c => "_" + c(0) + c(1) }.mkString.toLowerCase

      // three byte UTF8 chars result in 9 char string
      enc("你").length mustEqual 9
      enc("好").length mustEqual 9

      val encodedSFT = "nihao" + enc("你") + enc("好")
      encodedSFT mustEqual AccumuloDataStore.hexEncodeNonAlphaNumeric(sftName)

      AccumuloDataStore.formatSpatioTemporalIdxTableName(table, sft) mustEqual s"${table}_${encodedSFT}_st_idx"
      AccumuloDataStore.formatRecordTableName(table, sft) mustEqual s"${table}_${encodedSFT}_records"
      AccumuloDataStore.formatAttrIdxTableName(table, sft) mustEqual s"${table}_${encodedSFT}_attr_idx"

      c.tableOperations().exists(table) must beTrue
      c.tableOperations().exists(s"${table}_${encodedSFT}_st_idx") must beTrue
      c.tableOperations().exists(s"${table}_${encodedSFT}_records") must beTrue
      c.tableOperations().exists(s"${table}_${encodedSFT}_attr_idx") must beTrue
    }

  }

  def getFeatures(sft: SimpleFeatureType) = (0 until 6).map { i =>
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.set("geom", WKTUtils.read("POINT(45.0 45.0)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }

  "AccumuloFeatureStore" should {
    "compute target schemas from transformation expressions" in {
      val origSFT = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      origSFT.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
      val definitions =
        TransformProcess.toDefinition("name=name;helloName=strConcat('hello', name);geom=geom")

      val result = AccumuloFeatureStore.computeSchema(origSFT, definitions.toSeq)
      println(SimpleFeatureTypes.encodeType(result))

      (result must not).beNull
    }
  }

}
