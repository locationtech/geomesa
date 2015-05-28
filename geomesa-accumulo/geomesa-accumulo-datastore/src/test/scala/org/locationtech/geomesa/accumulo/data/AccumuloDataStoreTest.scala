/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.accumulo.data

import java.text.SimpleDateFormat
import java.util.Date

import com.vividsolutions.jts.geom.Coordinate
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Range}
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.{DefaultFeatureCollection, NameImpl}
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.CRS
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.{IndexIterator, TestData}
import org.locationtech.geomesa.accumulo.util.{CloseableIterator, GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.security.{AuthorizationsProvider, DefaultAuthorizationsProvider, FilteringAuthorizationsProvider}
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreTest extends Specification with AccumuloDataStoreDefaults {

  "AccumuloDataStore" should {
    "create a store" in {
      ds must not beNull
    }
    "create a schema" in {
      val sftName = "createSchemaTest"
      val sft = createSchema(sftName)
      ds.getSchema(sftName) mustEqual sft
    }
    "create and retrieve a schema with a custom IndexSchema" in {
      val sftName = "schematestCustomSchema"
      val indexSchema =
        new IndexSchemaBuilder("~")
          .randomNumber(3)
          .indexOrDataFlag()
          .constant(sftName)
          .geoHash(0, 3)
          .date("yyyyMMdd")
          .nextPart()
          .geoHash(3, 2)
          .nextPart()
          .id()
          .build()
      val sft = SimpleFeatureTypes.createType(sftName, defaultSchema)
      sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
      sft.getUserData.put(SFT_INDEX_SCHEMA, indexSchema)
      ds.createSchema(sft)

      val retrievedSft = ds.getSchema(sftName)

      retrievedSft must equalTo(sft)
      retrievedSft.getUserData.get(SF_PROPERTY_START_TIME) mustEqual "dtg"
      retrievedSft.getUserData.get(SFT_INDEX_SCHEMA) must beEqualTo(indexSchema)
      getIndexSchema(retrievedSft) must beEqualTo(Option(indexSchema))
    }
    "create and retrieve a schema without a custom IndexSchema" in {
      val sftName = "schematestDefaultSchema"
      val sft = SimpleFeatureTypes.createType(sftName, defaultSchema)
      sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

      val mockMaxShards = ds.DEFAULT_MAX_SHARD
      val indexSchema = ds.computeSpatioTemporalSchema(sft, mockMaxShards)

      ds.createSchema(sft)

      val retrievedSft = ds.getSchema(sftName)

      mockMaxShards mustEqual 0
      retrievedSft mustEqual sft
      retrievedSft.getUserData.get(SF_PROPERTY_START_TIME) mustEqual "dtg"
      retrievedSft.getUserData.get(SFT_INDEX_SCHEMA) mustEqual indexSchema
      getIndexSchema(retrievedSft) mustEqual Option(indexSchema)
    }
    "return NULL when a feature name does not exist" in {
      ds.getSchema("testTypeThatDoesNotExist") must beNull
    }
    "return type names" in {
      val sftName = "typeNameTest"
      val sft = createSchema(sftName)
      ds.getTypeNames.toSeq must contain(sftName)
    }

    "provide ability to write using the feature source and read what it wrote" in {
      val sftName = "featureSourceTest"
      val sft = createSchema(sftName)

      val res = addDefaultPoint(sft)

      // compose a CQL query that uses a reasonably-sized polygon for searching
      val cqlFilter = CQL.toFilter(s"BBOX(geom, 44.9,48.9,45.1,49.1)")
      val query = new Query(sftName, cqlFilter)

      // Let's read out what we wrote.
      val results = ds.getFeatureSource(sftName).getFeatures(query)
      val features = results.features
      var containsGeometry = false

      while (features.hasNext) {
        containsGeometry = containsGeometry | features.next.getDefaultGeometry.equals(defaultGeom)
      }

      "results schema should match" >> { results.getSchema mustEqual sft }
      "geometry should be set" >> { containsGeometry must beTrue }
      "result length should be 1" >> { res must haveLength(1) }
    }

    "return an empty iterator correctly" in {
      val sftName = "emptyIterTest"
      val sft = createSchema(sftName)
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
      "after writing 1 feature" >> { res should haveLength(1) }

      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val cqlFilter = CQL.toFilter(s"BBOX(geom, 64.9,68.9,65.1,69.1)")
      val query = new Query(sftName, cqlFilter)

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features

      "where schema matches" >> { results.getSchema mustEqual sft }
      "and there are no results" >> { features.hasNext must beFalse }
    }

    "create a schema with custom record splitting options" in {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;table.splitter.class=" +
          s"${classOf[DigitSplitter].getName},table.splitter.options=fmt:%02d,min:0,max:99"
      val sft = SimpleFeatureTypes.createType("customsplit", spec)
      org.locationtech.geomesa.accumulo.index.setTableSharing(sft, false)
      ds.createSchema(sft)
      val recTable = ds.getRecordTable(sft)
      val splits = ds.connector.tableOperations().listSplits(recTable)
      splits.size() mustEqual 100
      splits.head mustEqual new Text("00")
      splits.last mustEqual new Text("99")
    }

    "allow for a configurable number of threads in z3 queries" in {
      val sftName = "z3threads"
      val sft = createSchema(sftName)
      val param = AccumuloDataStoreFactory.params.queryThreadsParam.getName
      val query = new Query(sftName, ECQL.toFilter("bbox(geom,-75,-75,-60,-60) AND " +
          "dtg DURING 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z"))
      def testThreads(numThreads: Int) = {
        val params = dsParams ++ Map(param -> numThreads)
        val dst = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
        val qpt = dst.getQueryPlan(sftName, query)
        qpt must haveSize(1)
        qpt.head.table mustEqual dst.getZ3Table(sftName)
        qpt.head.numThreads mustEqual numThreads
      }

      forall(Seq(1, 5, 8, 20, 100))(testThreads)

      // check default
      val qpt = ds.getQueryPlan(sftName, query)
      qpt must haveSize(1)
      qpt.head.table mustEqual ds.getZ3Table(sftName)
      qpt.head.numThreads mustEqual 8
    }

    "process a DWithin query correctly" in {
      // create the data store
      val sftName = "dwithintest"
      val sft = createSchema(sftName)

      addDefaultPoint(sft)

      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val geomFactory = JTSFactoryFinder.getGeometryFactory
      val q = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate(45.000001, 48.99999))), 100.0, "meters")
      val query = new Query(sftName, q)

      // Let's read out what we wrote.
      val results = ds.getFeatureSource(sftName).getFeatures(query)
      val features = results.features

      "with correct result" >> {
        features.hasNext must beTrue
        features.next().getID mustEqual "fid-1"
        features.hasNext must beFalse
      }
    }

    "handle bboxes without property name" in {
      val sftName = "bboxproptest"
      val sft = createSchema(sftName)

      addDefaultPoint(sft)

      val filterNull = ff.bbox(ff.property(null.asInstanceOf[String]), 40, 44, 50, 54, "EPSG:4326")
      val filterEmpty = ff.bbox(ff.property(""), 40, 44, 50, 54, "EPSG:4326")
      val queryNull = new Query(sftName, filterNull)
      val queryEmpty = new Query(sftName, filterEmpty)

      val explainNull = {
        val o = new ExplainString
        ds.explainQuery(sftName, queryNull, o)
        o.toString()
      }
      val explainEmpty = {
        val o = new ExplainString
        ds.explainQuery(sftName, queryEmpty, o)
        o.toString()
      }

      explainNull must contain("Geometry filters: List([ null bbox POLYGON ((40 44, 40 54, 50 54, 50 44, 40 44)) ])")
      explainEmpty must contain("Geometry filters: List([  bbox POLYGON ((40 44, 40 54, 50 54, 50 44, 40 44)) ])")

      val featuresNull = ds.getFeatureSource(sftName).getFeatures(queryNull).features.toSeq.map(_.getID)
      val featuresEmpty = ds.getFeatureSource(sftName).getFeatures(queryEmpty).features.toSeq.map(_.getID)

      featuresNull mustEqual Seq("fid-1")
      featuresEmpty mustEqual Seq("fid-1")
    }

    "process an OR query correctly" in {
      val sftName = "ortest"
      val sft = createSchema(sftName)

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      {
        val randVal: (Double, Double) => Double = {
          val r = new Random(System.nanoTime())
          (low, high) => {
            (r.nextDouble() * (high - low)) + low
          }
        }
        val fc = new DefaultFeatureCollection(sftName, sft)
        for (i <- 0 until 1000) {
          val lat = randVal(-0.001, 0.001)
          val lon = randVal(-0.001, 0.001)
          val geom = WKTUtils.read(s"POINT($lat $lon)")
          val builder = new SimpleFeatureBuilder(sft, featureFactory)
          builder.addAll(List("testType", geom, null))
          val feature = builder.buildFeature(s"fid-$i")
          feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          fc.add(feature)
        }
        fs.addFeatures(fc)
      }

      val geomFactory = JTSFactoryFinder.getGeometryFactory
      val urq = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate( 0.0005,  0.0005))), 150.0, "meters")
      val llq = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate(-0.0005, -0.0005))), 150.0, "meters")
      val orq = ff.or(urq, llq)
      val andq = ff.and(urq, llq)
      val urQuery  = new Query(sftName,  urq)
      val llQuery  = new Query(sftName,  llq)
      val orQuery  = new Query(sftName,  orq)
      val andQuery = new Query(sftName, andq)

      val urNum  = fs.getFeatures(urQuery).features.length
      val llNum  = fs.getFeatures(llQuery).features.length
      val orNum  = fs.getFeatures(orQuery).features.length
      val andNum = fs.getFeatures(andQuery).features.length

      "obeying inclusion-exclusion principle" >> {
        (urNum + llNum) mustEqual (orNum + andNum)
      }
    }

    "handle transformations" in {
      val sftName = "transformtest1"
      val sft = createSchema(sftName)

      addDefaultPoint(sft)

      val query = new Query(sftName, Filter.INCLUDE,
        Array("name", "derived=strConcat('hello',name)", "geom"))

      // Let's read out what we wrote.
      val results = ds.getFeatureSource(sftName).getFeatures(query)

      "with the correct schema" >> {
        SimpleFeatureTypes.encodeType(results.getSchema) mustEqual
            s"name:String,*geom:Point:srid=4326:$OPT_INDEX=full:$OPT_INDEX_VALUE=true,derived:String"
      }
      "with the correct results" >> {
        val features = results.features
        features.hasNext must beTrue
        val f = features.next()
        DataUtilities.encodeFeature(f) mustEqual "fid-1=testType|POINT (45 49)|hellotestType"
      }
    }

    "handle transformations with dtg and geom" in {
      val sftName = "transformtest2"
      val sft = createSchema(sftName)

      addDefaultPoint(sft)

      val query = new Query(sftName, Filter.INCLUDE, List("dtg", "geom").toArray)
      val results = SelfClosingIterator(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())).toList
      results must haveSize(1)
      results(0).getAttribute("dtg") mustEqual(defaultDtg)
      results(0).getAttribute("geom") mustEqual(defaultGeom)
      results(0).getAttribute("name") must beNull
    }

    "handle back compatible transformations" in {
      val sftName = "transformtest7"
      val sft = createSchema(sftName)
      ds.setGeomesaVersion(sftName, 2)

      addDefaultPoint(sft)

      val query = new Query(sftName, Filter.INCLUDE, List("dtg", "geom").toArray)
      val results = SelfClosingIterator(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())).toList
      results must haveSize(1)
      results(0).getAttribute("dtg") mustEqual(defaultDtg)
      results(0).getAttribute("geom") mustEqual(defaultGeom)
      results(0).getAttribute("name") must beNull
    }

    "handle setPropertyNames transformations" in {
      val sftName = "transformtest3"
      val sft = createSchema(sftName)

      addDefaultPoint(sft)

      val filter = ff.bbox("geom", 44.0, 48.0, 46.0, 50.0, "EPSG:4326")
      val query = new Query(sftName, filter)
      query.setPropertyNames(Array("geom"))

      val features = ds.getFeatureSource(sftName).getFeatures(query).features

      val results = features.toList

      "return exactly one result" >> { results.size  must equalTo(1) }
      "with correct fields" >> {
        results(0).getAttribute("geom") mustEqual(defaultGeom)
        results(0).getAttribute("dtg") must beNull
        results(0).getAttribute("name") must beNull
      }
    }

    "handle transformations across multiple fields" in {
      val sftName = "transformtest4"
      val sft = createSchema(sftName, s"name:String,attr:String,dtg:Date,*geom:Point:srid=4326")

      addDefaultPoint(sft, List(defaultName, "v1", null, defaultGeom))

      val query = new Query(sftName, Filter.INCLUDE,
        Array("name", "derived=strConcat(attr,name)", "geom"))

      // Let's read out what we wrote.
      val results = ds.getFeatureSource(sftName).getFeatures(query)

      "with the correct schema" >> {
        SimpleFeatureTypes.encodeType(results.getSchema) mustEqual
            s"name:String,*geom:Point:srid=4326:$OPT_INDEX=full:$OPT_INDEX_VALUE=true,derived:String"
      }

      "with the correct results" >> {
        val features = results.features
        features.hasNext must beTrue
        val f = features.next()
        DataUtilities.encodeFeature(f) mustEqual "fid-1=testType|POINT (45 49)|v1testType"
      }
    }

    "handle transformations to subtypes" in {
      val sftName = "transformtest5"
      val sft = createSchema(sftName, s"name:String,attr:String,dtg:Date,*geom:Point:srid=4326")

      addDefaultPoint(sft, List(defaultName, "v1", null, defaultGeom))

      val query = new Query(sftName, Filter.INCLUDE, Array("name", "geom"))

      // Let's read out what we wrote.
      val results = ds.getFeatureSource(sftName).getFeatures(query)

      "with the correct schema" >> {
        SimpleFeatureTypes.encodeType(results.getSchema) mustEqual
            s"name:String,*geom:Point:srid=4326:$OPT_INDEX=full:$OPT_INDEX_VALUE=true"
      }
      "with the correct results" >> {
        val features = results.features
        features.hasNext must beTrue
        val f = features.next()
        DataUtilities.encodeFeature(f) mustEqual "fid-1=testType|POINT (45 49)"
      }
    }

    "handle transformations with filters on other attributes" in {
      val sftName = "transformtest6"
      val sft = createSchema(sftName, s"name:String,attr:String,dtg:Date,*geom:Point:srid=4326")

      val geom = WKTUtils.read("POINT(50.0 49.0)")
      val dtg = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse("2014-01-01T12:30:00.000+0000")
      addDefaultPoint(sft, List(defaultName, "v1", dtg, geom), "fid-1xxx")

      val filter =
        CQL.toFilter("bbox(geom,45,45,55,55) AND dtg BETWEEN '2013-01-01T00:00:00.000Z' AND '2015-01-02T00:00:00.000Z'")
      val query = new Query(sftName, filter, Array("geom"))

      // Let's read out what we wrote.
      val features = ds.getFeatureSource(sftName).getFeatures(query).features
      "return the data" >> {
        features.hasNext must beTrue
      }
      "with correct results" >> {
        val f = features.next()
        DataUtilities.encodeFeature(f) mustEqual "fid-1xxx=POINT (50 49)"
      }
    }

    "handle between intra-day queries" in {
      val sftName = "betweenTest"
      val sft = createSchema(sftName)

      val geom = WKTUtils.read("POINT(50.0 49.0)")
      val dtg = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse("2014-01-01T12:30:00.000+0000")
      addDefaultPoint(sft, List(defaultName, geom, dtg), "fid-2")

      val filter =
        CQL.toFilter("bbox(geom,40,40,60,60) AND dtg BETWEEN '2014-01-01T12:00:00.000Z' AND '2014-01-01T13:00:00.000Z'")
      val query = new Query(sftName, filter)

      // Let's read out what we wrote.
      val features = ds.getFeatureSource(sftName).getFeatures(query).features
      features.hasNext must beTrue
      val f = features.next()
      DataUtilities.encodeFeature(f) mustEqual "fid-2=testType|POINT (50 49)|2014-01-01T12:30:00.000Z"
      features.hasNext must beFalse
    }

    "handle requests with namespaces" in {
      // create the data store
      val ns = "mytestns"
      val sftName = "namespacetest"
      val sft = createSchema(sftName)

      val schemaWithoutNs = ds.getSchema(sftName)

      schemaWithoutNs.getName.getNamespaceURI must beNull
      schemaWithoutNs.getName.getLocalPart mustEqual sftName

      val schemaWithNs = ds.getSchema(new NameImpl(ns, sftName))

      schemaWithNs.getName.getNamespaceURI mustEqual ns
      schemaWithNs.getName.getLocalPart mustEqual sftName
    }

    "handle IDL correctly" in {
      val sftName = "IDLTest"
      val sft = createSchema(sftName, TestData.getTypeSpec())

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      val featureCollection = new DefaultFeatureCollection()
      featureCollection.addAll(TestData.allThePoints.map(TestData.createSF))
      fs.addFeatures(featureCollection)

      "default layer preview, bigger than earth, multiple IDL-wrapping geoserver BBOX" in {
        val spatial = ff.bbox("geom", -230, -110, 230, 110, CRS.toSRS(WGS84))
        val query = new Query(sftName, spatial)
        val results = fs.getFeatures(query)
        results.size() mustEqual 361
      }

      "greater than 180 lon diff non-IDL-wrapping geoserver BBOX" in {
        val spatial = ff.bbox("geom", -100, 1.1, 100, 4.1, CRS.toSRS(WGS84))
        val query = new Query(sftName, spatial)
        val results = fs.getFeatures(query)
        results.size() mustEqual 6
      }

      "small IDL-wrapping geoserver BBOXes" in {
        val spatial1 = ff.bbox("geom", -181.1, -90, -175.1, 90, CRS.toSRS(WGS84))
        val spatial2 = ff.bbox("geom", 175.1, -90, 181.1, 90, CRS.toSRS(WGS84))
        val binarySpatial = ff.or(spatial1, spatial2)
        val query = new Query(sftName, binarySpatial)
        val results = fs.getFeatures(query)
        results.size() mustEqual 10
      }

      "large IDL-wrapping geoserver BBOXes" in {
        val spatial1 = ff.bbox("geom", -181.1, -90, 40.1, 90, CRS.toSRS(WGS84))
        val spatial2 = ff.bbox("geom", 175.1, -90, 181.1, 90, CRS.toSRS(WGS84))
        val binarySpatial = ff.or(spatial1, spatial2)
        val query = new Query(sftName, binarySpatial)
        val results = fs.getFeatures(query)
        results.size() mustEqual 226
      }

    }

    "provide ability to configure authorizations" in {

      sequential

      "by static auths" in {
        // create the data store
        val ds = DataStoreFinder.getDataStore(Map(
          "instanceId" -> "mycloud",
          "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user" -> "myuser",
          "password" -> "mypassword",
          "auths" -> "user",
          "tableName" -> "testwrite",
          "useMock" -> "true",
          "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.authorizationsProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.authorizationsProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must
            beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.authorizationsProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations mustEqual
            new Authorizations("user")
      }

      "by comma-delimited static auths" in {
        // create the data store
        val ds = DataStoreFinder.getDataStore(Map(
          "instanceId" -> "mycloud",
          "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user" -> "myuser",
          "password" -> "mypassword",
          "auths" -> "user,admin,test",
          "tableName" -> "testwrite",
          "useMock" -> "true",
          "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.authorizationsProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.authorizationsProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must
            beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.authorizationsProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations mustEqual
            new Authorizations("user", "admin", "test")
      }

      "fail when auth provider system property does not match an actual class" in {
        System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY, "my.fake.Clas")
        try {
          // create the data store
          DataStoreFinder.getDataStore(Map(
            "instanceId"        -> "mycloud",
            "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
            "user"              -> "myuser",
            "password"          -> "mypassword",
            "auths"             -> "user,admin,test",
            "tableName"         -> "testwrite",
            "useMock"           -> "true",
            "featureEncoding"   -> "avro")) should throwA[IllegalArgumentException]
        } finally System.clearProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY)
      }
    }

    "allow users with sufficient auths to write data" in {
      val sftName = "authwritetest1"
      // create the data store
      val ds = DataStoreFinder.getDataStore(Map(
        "instanceId"        -> "mycloud",
        "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"              -> "myuser",
        "password"          -> "mypassword",
        "auths"             -> "user,admin",
        "visibilities"      -> "user&admin",
        "tableName"         -> "testwrite",
        "useMock"           -> "true",
        "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]
      ds should not be null

      // create the schema - the auths for this user are sufficient to write data
      val sft = createSchema(sftName, dataStore = ds)

      // write some data
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      val written = fs.addFeatures(new ListFeatureCollection(sft, createTestFeatures(sft).toList))

      written must not beNull;
      written.length mustEqual 6
    }

    "restrict users with insufficient auths from writing data" in {
      val sftName = "authwritetest2"
      // create the data store
      val ds = DataStoreFinder.getDataStore(Map(
        "instanceId"        -> "mycloud",
        "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"              -> "myuser",
        "password"          -> "mypassword",
        "auths"             -> "user",
        "visibilities"      -> "user&admin",
        "tableName"         -> "testwrite",
        "useMock"           -> "true",
        "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]
      ds must not beNull

      // create the schema - the auths for this user are less than the visibility used to write data
      val sft = createSchema(sftName, dataStore = ds)

      // write some data
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      fs.addFeatures(new ListFeatureCollection(sft, createTestFeatures(sft).toList)) must throwA[RuntimeException]
    }

    "allow users to call explainQuery" in {
      val sftName = "explainQueryTest"
      createSchema(sftName)
      val query = new Query(sftName, Filter.INCLUDE)
      val fr = ds.getFeatureReader(sftName)
      fr must not beNull;
      val out = new ExplainString
      ds.explainQuery(sftName, new Query(sftName, Filter.INCLUDE), out)
      val explain = out.toString()
      explain must startWith(s"Planning Query")
    }

    "allow secondary attribute indexes" in {
      val sftName = "AttributeIndexTest"
      val sft = createSchema(sftName, "name:String:index=true,numattr:Integer,dtg:Date,*geom:Point:srid=4326")

      val c = ds.connector

      "create all appropriate tables" >> {
        "catalog table" >> { c.tableOperations().exists(defaultTable) must beTrue }
        "st idx table" >> { c.tableOperations().exists(s"${defaultTable}_st_idx") must beTrue }
        "records table" >> { c.tableOperations().exists(s"${defaultTable}_records") must beTrue }
        "attr idx table" >> { c.tableOperations().exists(s"${defaultTable}_attr_idx") must beTrue }
      }

      val pt = gf.createPoint(new Coordinate(0, 0))
      val one = AvroSimpleFeatureFactory.buildAvroFeature(sft, Seq("one", new Integer(1), new DateTime(), pt), "1")
      val two = AvroSimpleFeatureFactory.buildAvroFeature(sft, Seq("two", new Integer(2), new DateTime(), pt), "2")

      val fs = ds.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]
      fs.addFeatures(DataUtilities.collection(List(one, two)))

      "query indexed attribute" >> {
        val q1 = ff.equals(ff.property("name"), ff.literal("one"))
        val fr = ds.getFeatureReader(sftName, new Query(sftName, q1))
        val results = CloseableIterator(fr).toList
        results must haveLength(1)
        results.head.getAttribute("name") mustEqual "one"
      }

      "query non-indexed attributes" >> {
        val q2 = ff.equals(ff.property("numattr"), ff.literal(2))
        val fr = ds.getFeatureReader(sftName, new Query(sftName, q2))
        val results = CloseableIterator(fr).toList
        results must haveLength(1)
        results.head.getAttribute("numattr") mustEqual 2
      }
    }

    "support caching for improved WFS performance due to count/getFeatures" in {
      val sftName = "testingCaching"
      val ds = DataStoreFinder.getDataStore(Map(
        "instanceId"        -> "mycloud",
        "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"              -> "myuser",
        "password"          -> "mypassword",
        "tableName"         -> defaultTable,
        "caching"           -> true,
        "useMock"           -> "true",
        "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

      val sft = createSchema(sftName, dataStore = ds)
      addDefaultPoint(sft, dataStore = ds, fid = "f1")
      addDefaultPoint(sft, dataStore = ds, fid = "f2")

      "typeOf feature source must be CachingAccumuloFeatureCollection" >> {
        val fc = ds.getFeatureSource(sftName).getFeatures(Filter.INCLUDE)
        fc must haveClass[CachingAccumuloFeatureCollection]
      }

      "suport getCount" >> {
        val query = new Query(sftName, Filter.INCLUDE)
        val fs = ds.getFeatureSource(sftName)
        val count = fs.getCount(query)
        count mustEqual 2
        val features = SelfClosingIterator(fs.getFeatures(query).features()).toList
        features must haveSize(2)
        features.map(_.getID) must containAllOf(Seq("f1", "f2"))
      }
    }

    "hex encode multibyte chars as multiple underscore + hex" in {
      // accumulo supports only alphanum + underscore aka ^\\w+$
      // this should end up hex encoded
      val sftName = "nihao你好"
      val sft = SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
      org.locationtech.geomesa.accumulo.index.setTableSharing(sft, false)
      ds.createSchema(sft)

      // encode groups of 2 hex chars since we are doing multibyte chars
      def enc(s: String): String = Hex.encodeHex(s.getBytes("UTF8")).grouped(2)
        .map{ c => "_" + c(0) + c(1) }.mkString.toLowerCase

      // three byte UTF8 chars result in 9 char string
      enc("你") must haveLength(9)
      enc("好") must haveLength(9)

      val encodedSFT = "nihao" + enc("你") + enc("好")
      encodedSFT mustEqual AccumuloDataStore.hexEncodeNonAlphaNumeric(sftName)

      AccumuloDataStore.formatSpatioTemporalIdxTableName(defaultTable, sft) mustEqual
          s"${defaultTable}_${encodedSFT}_st_idx"
      AccumuloDataStore.formatRecordTableName(defaultTable, sft) mustEqual
          s"${defaultTable}_${encodedSFT}_records"
      AccumuloDataStore.formatAttrIdxTableName(defaultTable, sft) mustEqual
          s"${defaultTable}_${encodedSFT}_attr_idx"

      val c = ds.connector

      c.tableOperations().exists(defaultTable) must beTrue
      c.tableOperations().exists(s"${defaultTable}_${encodedSFT}_st_idx") must beTrue
      c.tableOperations().exists(s"${defaultTable}_${encodedSFT}_records") must beTrue
      c.tableOperations().exists(s"${defaultTable}_${encodedSFT}_attr_idx") must beTrue
    }

    "support deleting schemas" in {

      def scanMetadata(ds: AccumuloDataStore, sftName: String): Option[String] = {
        val scanner = ds.connector.createScanner(ds.catalogTable, ds.authorizationsProvider.getAuthorizations)
        scanner.setRange(new Range(s"${METADATA_TAG }_$sftName"))

        val name = "version-" + sftName
        val cfg = new IteratorSetting(1, name, classOf[VersioningIterator])
        VersioningIterator.setMaxVersions(cfg, 1)
        scanner.addScanIterator(cfg)

        val iter = scanner.iterator
        val result =
          if (iter.hasNext) {
            Some(iter.next.getValue.toString)
          } else {
            None
          }

        scanner.close()
        scanner.removeScanIterator(name)
        result
      }

      def buildPreSecondaryIndexTable(params: Map[String, String], sftName: String) = {
        val rowIds = List(
          "09~regressionTestType~v00~20120102",
          "95~regressionTestType~v00~20120102",
          "53~regressionTestType~v00~20120102",
          "77~regressionTestType~v00~20120102",
          "36~regressionTestType~v00~20120102",
          "91~regressionTestType~v00~20120102")
        val hex = new Hex
        val indexValues = List(
          "000000013000000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013100000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013200000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013300000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013400000015000000000140468000000000004046800000000000000001349ccf6e18",
          "000000013500000015000000000140468000000000004046800000000000000001349ccf6e18").map {v =>
          hex.decode(v.getBytes)}
        val sft = SimpleFeatureTypes.createType(sftName, s"name:String,$geotimeAttributes")
        sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

        val instance = new MockInstance(params("instanceId"))
        val connector = instance.getConnector(params("user"), new PasswordToken(params("password").getBytes))
        connector.tableOperations.create(params("tableName"))

        val bw = connector.createBatchWriter(params("tableName"), GeoMesaBatchWriterConfig())

        // Insert metadata
        val metadataMutation = new Mutation(s"~METADATA_$sftName")
        metadataMutation.put("attributes", "", "name:String,geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date")
        metadataMutation.put("bounds", "", "45.0:45.0:49.0:49.0")
        metadataMutation.put("schema", "", s"%~#s%99#r%$sftName#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id")
        bw.addMutation(metadataMutation)

        // Insert features
        createTestFeatures(sft).zipWithIndex.foreach { case(sf, idx) =>
          val encoded = DataUtilities.encodeFeature(sf)
          val index = new Mutation(rowIds(idx))
          index.put("00".getBytes,sf.getID.getBytes, indexValues(idx))
          bw.addMutation(index)

          val data = new Mutation(rowIds(idx))
          data.put(sf.getID, "SimpleFeatureAttribute", encoded)
          bw.addMutation(data)
        }

        bw.flush
        bw.close
      }

      def createFeature(sftName: String, ds: AccumuloDataStore, sharedTables: Boolean = true) = {
        val sft = SimpleFeatureTypes.createType(sftName, defaultSchema)
        org.locationtech.geomesa.accumulo.index.setTableSharing(sft, sharedTables)
        ds.createSchema(sft)
        addDefaultPoint(sft, dataStore = ds)
      }

      "delete the schema completely" in {
        val sftName = "deleteSchemaTest"
        val table = "testing_delete_schema"
        val ds = DataStoreFinder.getDataStore(Map(
          "instanceId"        -> "mycloud",
          "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user"              -> "myuser",
          "password"          -> "mypassword",
          "tableName"         -> table,
          "useMock"           -> "true")).asInstanceOf[AccumuloDataStore]

        ds must not beNull

        createFeature(sftName, ds, false)

        val c = ds.connector

        // tests that tables exist before being deleted
        c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_records") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beTrue

        val fr = ds.getFeatureReader(sftName)
        // tests that metadata exists in the catalog before being deleted
        fr must not beNull

        scanMetadata(ds, sftName) should beSome

        ds.removeSchema(sftName)

        //tables should be deleted now (for stand-alone tables only)
        c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beFalse
        c.tableOperations().exists(s"${table}_${sftName}_records") must beFalse
        c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beFalse

        //metadata should be deleted from the catalog now
          scanMetadata(ds, sftName) should beNone

        val query = new Query(sftName, Filter.INCLUDE)
        ds.getFeatureSource(sftName).getFeatures(query) must throwA[Exception]
      }

      "throw a RuntimeException when calling removeSchema on 0.10.x records" in {
        val sftName = "regressionRemoveSchemaTest"

        val manualParams = Map(
          "instanceId" -> "mycloud",
          "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user"       -> "myuser",
          "password"   -> "mypassword",
          "auths"      -> "A,B,C",
          "useMock"    -> "true",
          "tableName"  -> "manualTableForDeletion")

        buildPreSecondaryIndexTable(manualParams, sftName)

        val manualStore = DataStoreFinder.getDataStore(manualParams).asInstanceOf[AccumuloDataStore]
        manualStore.removeSchema(sftName) should throwA[RuntimeException]
      }

      "keep other tables when a separate schema is deleted" in {
        val sftName = "deleteSharedSchemaTest"
        val sftName2 = "deleteSharedSchemaTest2"

        val table = "testing_shared_delete_schema"
        val ds = DataStoreFinder.getDataStore(Map(
          "instanceId"        -> "mycloud",
          "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
          "user"              -> "myuser",
          "password"          -> "mypassword",
          "tableName"         -> table,
          "useMock"           -> "true")).asInstanceOf[AccumuloDataStore]

        ds should not beNull

        createFeature(sftName, ds, false)
        createFeature(sftName2, ds, false)

        val c = ds.connector

        //tests that tables exist before being deleted
        c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_records") must beTrue
        c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_records") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_attr_idx") must beTrue

        val fr = ds.getFeatureReader(sftName)
        val fr2 = ds.getFeatureReader(sftName2)
        //tests that metadata exists in the catalog before being deleted
        fr should not be null
        fr2 should not be null

        val scannerResults = scanMetadata(ds, sftName)
        val scannerResults2 = scanMetadata(ds, sftName2)
        scannerResults should beSome
        scannerResults2 should beSome

        ds.removeSchema(sftName)

        //these tables should be deleted now
        c.tableOperations().exists(s"${table}_${sftName}_st_idx") must beFalse
        c.tableOperations().exists(s"${table}_${sftName}_records") must beFalse
        c.tableOperations().exists(s"${table}_${sftName}_attr_idx") must beFalse
        //but these tables should still exist since sftName2 wasn't deleted
        c.tableOperations().exists(s"${table}_${sftName2}_st_idx") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_records") must beTrue
        c.tableOperations().exists(s"${table}_${sftName2}_attr_idx") must beTrue

        val scannerResultsAfterDeletion = scanMetadata(ds, sftName)
        val scannerResultsAfterDeletion2 = scanMetadata(ds, sftName2)

        //metadata should be deleted from the catalog now for sftName
        scannerResultsAfterDeletion should beNone
        //metadata should still exist for sftName2
        scannerResultsAfterDeletion2 should beSome

        val query2 = new Query(sftName2, Filter.INCLUDE)

        val results2 = ds.getFeatureSource(sftName2).getFeatures(query2)
        results2.size() should beGreaterThan(0)
      }
    }

    "update metadata for indexed attributes" in {
      val sftName = "updateMetadataTest"
      val originalSchema = s"name:String,dtg:Date,*geom:Point:srid=4326:$OPT_INDEX=full:$OPT_INDEX_VALUE=true"
      val updatedSchema = s"name:String:$OPT_INDEX=join,dtg:Date,*geom:Point:srid=4326:$OPT_INDEX=full:$OPT_INDEX_VALUE=true"

      val sft = createSchema(sftName, originalSchema)
      ds.updateIndexedAttributes(sftName, updatedSchema)
      val retrievedSchema = SimpleFeatureTypes.encodeType(ds.getSchema(sftName))
      retrievedSchema mustEqual updatedSchema
    }

    "prevent changing schema types" in {
      val sftName = "preventSchemaChangeTest"
      val originalSchema = s"name:String,dtg:Date,*geom:Point:srid=4326:$OPT_INDEX=full:$OPT_INDEX_VALUE=true"
      val sft = createSchema(sftName, originalSchema)

      "prevent changing default geometry" in {
        val updatedSchema = "name:String,dtg:Date,geom:Point:srid=4326"
        ds.updateIndexedAttributes(sftName, updatedSchema) should throwA[IllegalArgumentException]
        val retrievedSchema = SimpleFeatureTypes.encodeType(ds.getSchema(sftName))
        retrievedSchema mustEqual originalSchema
      }
      "prevent changing attribute order" in {
        val updatedSchema = "dtg:Date,name:String,*geom:Point:srid=4326"
        ds.updateIndexedAttributes(sftName, updatedSchema) should throwA[IllegalArgumentException]
        val retrievedSchema = SimpleFeatureTypes.encodeType(ds.getSchema(sftName))
        retrievedSchema mustEqual originalSchema
      }
      "prevent adding attributes" in {
        val updatedSchema = "name:String,dtg:Date,*geom:Point:srid=4326,newField:String"
        ds.updateIndexedAttributes(sftName, updatedSchema) should throwA[IllegalArgumentException]
        val retrievedSchema = SimpleFeatureTypes.encodeType(ds.getSchema(sftName))
        retrievedSchema mustEqual originalSchema
      }
      "prevent removing attributes" in {
        val updatedSchema = "dtg:Date,*geom:Point:srid=4326"
        ds.updateIndexedAttributes(sftName, updatedSchema) should throwA[IllegalArgumentException]
        val retrievedSchema = SimpleFeatureTypes.encodeType(ds.getSchema(sftName))
        retrievedSchema mustEqual originalSchema
      }
    }

    "Provide a feature update implementation" in {
      val sftName = "featureUpdateTest"
      val sft = createSchema(sftName, "name:String,dtg:Date,*geom:Point:srid=4326")

      val features = createTestFeatures(sft)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      fs.addFeatures(new ListFeatureCollection(sft, features))

      val filter = ff.id(ff.featureId("2"))
      val writer = ds.getFeatureWriter(sftName, filter, Transaction.AUTO_COMMIT)
      writer.hasNext must beTrue
      val feat = writer.next
      feat.getID mustEqual "2"
      feat.getAttribute("name") mustEqual "2"
      feat.setAttribute("name", "2-updated")
      writer.write()
      writer.hasNext must beFalse
      writer.close()

      val reader = ds.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
      reader.hasNext must beTrue
      val updated = reader.next()
      reader.hasNext must beFalse
      reader.close()
      updated.getID mustEqual("2")
      updated.getAttribute("name") mustEqual "2-updated"
    }

    "allow caching to be configured" in {
      val sftName = "cachingTest"

      DataStoreFinder.getDataStore(Map(
        "instanceId"        -> "mycloud",
        "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"              -> "myuser",
        "password"          -> "mypassword",
        "auths"             -> "A,B,C",
        "tableName"         -> sftName,
        "useMock"           -> "true",
        "caching"           -> false,
        "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore].cachingConfig must beFalse

      DataStoreFinder.getDataStore(Map(
        "instanceId"        -> "mycloud",
        "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"              -> "myuser",
        "password"          -> "mypassword",
        "auths"             -> "A,B,C",
        "tableName"         -> sftName,
        "useMock"           -> "true",
        "caching"           -> true,
        "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore].cachingConfig must beTrue
    }

    "not use caching by default" in {
      val sftName = "cachingTest"
      val instance = new MockInstance
      val connector = instance.getConnector("user", new PasswordToken("pass".getBytes()))
      val params = Map(
        "connector" -> connector,
        "tableName" -> sftName)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
      ds.cachingConfig must beFalse
    }

    "not use caching by default with mocks" in {
      ds.cachingConfig must beFalse
    }

    "Allow extra attributes in the STIDX entries" in {
      val sftName = "StidxExtraAttributeTest"
      val spec =
        s"name:String:$OPT_INDEX_VALUE=true,dtg:Date:$OPT_INDEX_VALUE=true,*geom:Point:srid=4326,attr2:String"
      val sft = createSchema(sftName, spec)

      val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
      val features = (0 until 6).map { i =>
        builder.set("geom", WKTUtils.read(s"POINT(45.0 4$i.0)"))
        builder.set("dtg", s"2012-01-02T05:0$i:07.000Z")
        builder.set("name", i.toString)
        builder.set("attr2", "2-" + i.toString)
        val sf = builder.buildFeature(i.toString)
        sf.getUserData.update(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf
      }

      val baseTime = features(0).getAttribute("dtg").asInstanceOf[Date].getTime

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      fs.addFeatures(new ListFeatureCollection(sft, features))

      val query = new Query(sftName, ECQL.toFilter("BBOX(geom, 40.0, 40.0, 50.0, 50.0)"),
        Array("geom", "dtg", "name"))
      val reader = ds.getFeatureReader(sftName, query)

      // verify that the IndexIterator is getting used with the extra field
      val explain = {
        val out = new ExplainString
        ds.explainQuery(sftName, query, out)
        out.toString()
      }
      explain must contain(classOf[IndexIterator].getName)

      val read = SelfClosingIterator(reader).toList

      // verify that all the attributes came back
      read must haveSize(6)
      read.sortBy(_.getAttribute("name").asInstanceOf[String]).zipWithIndex.foreach { case (sf, i) =>
        sf.getAttributeCount mustEqual 3
        sf.getAttribute("name") mustEqual i.toString
        sf.getAttribute("geom") mustEqual WKTUtils.read(s"POINT(45.0 4$i.0)")
        sf.getAttribute("dtg").asInstanceOf[Date].getTime mustEqual baseTime + i * 60000
      }
      success
    }

    "Use IndexIterator when projecting to date/geom" in {
      val sftName = "StidxExtraAttributeTest2"
      val spec =
        s"name:String:$OPT_INDEX_VALUE=true,dtg:Date:$OPT_INDEX_VALUE=true,*geom:Point:srid=4326,attr2:String"
      val sft = createSchema(sftName, spec)

      val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
      val features = (0 until 6).map { i =>
        builder.set("geom", WKTUtils.read(s"POINT(45.0 4$i.0)"))
        builder.set("dtg", s"2012-01-02T05:0$i:07.000Z")
        builder.set("name", i.toString)
        builder.set("attr2", "2-" + i.toString)
        val sf = builder.buildFeature(i.toString)
        sf.getUserData.update(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf
      }

      val baseTime = features(0).getAttribute("dtg").asInstanceOf[Date].getTime

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      fs.addFeatures(new ListFeatureCollection(sft, features))

      val query = new Query(sftName, ECQL.toFilter("BBOX(geom, 40.0, 40.0, 50.0, 50.0)"),
        Array("geom", "dtg"))
      val reader = ds.getFeatureReader(sftName, query)

      // verify that the IndexIterator is getting used
      val explain = {
        val out = new ExplainString
        ds.explainQuery(sftName, query, out)
        out.toString()
      }
      explain must contain(classOf[IndexIterator].getName)

      val read = SelfClosingIterator(reader).toList

      // verify that all the attributes came back
      read must haveSize(6)
      read.sortBy(_.getAttribute("dtg").toString).zipWithIndex.foreach { case (sf, i) =>
        sf.getAttributeCount mustEqual 2
        sf.getAttribute("name") must beNull
        sf.getAttribute("geom") mustEqual WKTUtils.read(s"POINT(45.0 4$i.0)")
        sf.getAttribute("dtg").asInstanceOf[Date].getTime mustEqual baseTime + i * 60000
      }
      success
    }

    "create key plan that uses STII Filter with bbox" in {
      val sftName = "explainLargeBBOXTest1"
      val sft1 = createSchema(sftName)
      val filter = CQL.toFilter("bbox(geom, -100, -45, 100, 45)")
      val query = new Query(sftName, filter, Array("geom"))
      val explain = {
        val o = new ExplainString
        ds.explainQuery(sftName, query, o)
        o.toString()
      }
      ds.removeSchema(sftName)
      explain must contain("STII Filter: [ geom bbox POLYGON ((-100 -45, -100 45, 100 45, 100 -45, -100 -45)) ]")
    }

    "create key plan that does not use STII when given the Whole World bbox" in {
      val sftName = "explainLargeBBOXTest2"
      val sft2 = createSchema(sftName)
      val filter = CQL.toFilter("bbox(geom, -180, -90, 180, 90)")
      val query = new Query(sftName, filter, Array("geom"))
      val explain = {
        val o = new ExplainString
        ds.explainQuery(sftName, query, o)
        o.toString()
      }
      ds.removeSchema(sftName)
      explain must contain("No STII Filter")
      explain must contain("Filter: AcceptEverythingFilter")
    }

    "create key plan that does not use STII when given something larger than the Whole World bbox" in {
      val sftName = "explainLargeBBOXTest3"
      val sft3 = createSchema(sftName)
      val filter = CQL.toFilter("bbox(geom, -190, -100, 190, 100)")
      val query = new Query(sftName, filter, Array("geom"))
      val explain = {
        val o = new ExplainString
        ds.explainQuery(sftName, query, o)
        o.toString()
      }
      ds.removeSchema(sftName)
      explain must contain("No STII Filter")
      explain must contain("Filter: AcceptEverythingFilter")
    }

    "create key plan that does not use STII when given an or'd geometry query with redundant bbox" in {
      // Todo: https://geomesa.atlassian.net/browse/GEOMESA-785
      val sftName = "explainLargeBBOXTest4"
      val sft3 = createSchema(sftName)
      val filter = CQL.toFilter("bbox(geom, -180, -90, 180, 90) OR bbox(geom, -10, -10, 10, 10)")
      val query = new Query(sftName, filter, Array("geom"))
      val explain = {
        val o = new ExplainString
        ds.explainQuery(sftName, query, o)
        o.toString()
      }
      ds.removeSchema(sftName)
      explain must not contain("STII Filter: [ geom bbox ")
      explain must contain("No STII Filter")
      explain must contain("Filter: AcceptEverythingFilter")
    }.pendingUntilFixed("Fixed query planner to deal with OR'd redundant geom with whole world")

    "create key plan that does not use STII when given two bboxes that when unioned are the whole world" in {
      // Todo: https://geomesa.atlassian.net/browse/GEOMESA-785
      val sftName = "explainWhatIsLogicallyTheWholeWorldTest1"
      val sft4 = createSchema(sftName)
      val filter = CQL.toFilter("bbox(geom, -180, -90, 0, 90) OR bbox(geom, 0, -90, 180, 90)")
      val query = new Query(sftName, filter, Array("geom"))
      val explain = {
        val o = new ExplainString
        ds.explainQuery(sftName, query, o)
        o.toString()
      }
      ds.removeSchema(sftName)
      explain must not contain("STII Filter: [ geom bbox ")
      explain must contain("No STII Filter")
      explain must contain("Filter: AcceptEverythingFilter")
    }.pendingUntilFixed("Fixed query planner to deal with OR'd whole world geometry")

    "create key plan correctly with a large bbox and a date range" in {
      val sftName1 = "keyPlanTest1"
      val sftName2 = "keyPlanTest2"

      val sft1 = createSchema(sftName1)
      val sft2 = createSchema(sftName2)

      addDefaultPoint(sft1, fid = "fid-sft1")
      addDefaultPoint(sft2, fid = "fid-sft2")

      val filter =
        CQL.toFilter("bbox(geom,-180,-90,180,90) AND dtg BETWEEN '1969-12-31T00:00:00.000Z' AND '1970-01-02T00:00:00.000Z'")
      val query = new Query(sftName1, filter, Array("geom", "dtg"))

      // Let's read out what we wrote.
      val features = SelfClosingIterator(ds.getFeatureSource(sftName1).getFeatures(query).features).toList
      features.size mustEqual 1
      features.head.getID mustEqual "fid-sft1"

      val explain = {
        val o = new ExplainString
        ds.explainQuery(sftName1, query, o)
        o.toString()
      }
      explain must not contain "GeoHashKeyPlanner: KeyInvalid"
    }

    "transform index value data correctly" in {
      val sftName = "indexValueTransform"
      val sft = createSchema(sftName, "trackId:String:index-value=true,label:String:index-value=true," +
          "extraValue:String,score:Double:index-value=true,dtg:Date,geom:Point:srid=4326")

      (0 until 5).foreach { i =>
        val attrs = List(s"trk$i", s"label$i", "extra", new java.lang.Double(i),
          df.parse(s"2014-01-01 0$i:00:00"), WKTUtils.read(s"POINT(5$i 50)"))
        addDefaultPoint(sft, fid = s"f$i", attributes = attrs)
      }

      "with out of order attributes" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom,49,49,60,60)"), Array("geom", "dtg", "label"))
        val features =
          SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features must haveSize(5)
        (0 until 5).foreach { i =>
          features(i).getID mustEqual(s"f$i")
          features(i).getAttributeCount mustEqual 3
          features(i).getAttribute("label") mustEqual s"label$i"
          features(i).getAttribute("dtg") mustEqual df.parse(s"2014-01-01 0$i:00:00")
          features(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
        }
        success
      }

      "with only date and geom" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom,49,49,60,60)"), Array("geom", "dtg"))
        val features =
          SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features must haveSize(5)
        (0 until 5).foreach { i =>
          features(i).getID mustEqual(s"f$i")
          features(i).getAttributeCount mustEqual 2
          features(i).getAttribute("dtg") mustEqual df.parse(s"2014-01-01 0$i:00:00")
          features(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
        }
        success
      }

      "with all attributes" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom,49,49,60,60)"),
          Array("geom", "dtg", "label", "score", "trackId"))
        val features =
          SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
        features must haveSize(5)
        (0 until 5).foreach { i =>
          features(i).getID mustEqual(s"f$i")
          features(i).getAttributeCount mustEqual 5
          features(i).getAttribute("label") mustEqual s"label$i"
          features(i).getAttribute("trackId") mustEqual s"trk$i"
          features(i).getAttribute("score") mustEqual i.toDouble
          features(i).getAttribute("dtg") mustEqual df.parse(s"2014-01-01 0$i:00:00")
          features(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
        }
        success
      }
    }
  }
}
