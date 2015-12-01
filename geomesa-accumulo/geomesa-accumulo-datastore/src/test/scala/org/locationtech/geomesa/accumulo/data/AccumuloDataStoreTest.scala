/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.Date

import com.google.common.collect.ImmutableSet
import com.vividsolutions.jts.geom.Coordinate
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.IndexIterator
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreTest extends Specification with AccumuloDataStoreDefaults {

  sequential

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
      sft.setDtgField("dtg")
      sft.setStIndexSchema(indexSchema)
      ds.createSchema(sft)

      val retrievedSft = ds.getSchema(sftName)

      retrievedSft must equalTo(sft)
      retrievedSft.getDtgField must beSome("dtg")
      retrievedSft.getStIndexSchema mustEqual indexSchema
      retrievedSft.getStIndexSchema mustEqual indexSchema
    }
    "create and retrieve a schema without a custom IndexSchema" in {
      val sftName = "schematestDefaultSchema"
      val sft = SimpleFeatureTypes.createType(sftName, defaultSchema)
      sft.setDtgField("dtg")

      val mockMaxShards = ds.DEFAULT_MAX_SHARD
      val indexSchema = ds.computeSpatioTemporalSchema(sft)

      ds.createSchema(sft)

      val retrievedSft = ds.getSchema(sftName)

      mockMaxShards mustEqual 0
      retrievedSft mustEqual sft
      retrievedSft.getDtgField must beSome("dtg")
      retrievedSft.getStIndexSchema mustEqual indexSchema
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

    "create a schema with custom record splitting options with table sharing off" in {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;table.splitter.class=" +
          s"${classOf[DigitSplitter].getName},table.splitter.options='fmt:%02d,min:0,max:99'"
      val sft = SimpleFeatureTypes.createType("customsplit", spec)
      sft.setTableSharing(false)
      ds.createSchema(sft)
      val recTable = ds.getTableName(sft.getTypeName, RecordTable)
      val splits = ds.connector.tableOperations().listSplits(recTable)
      splits.size() mustEqual 100
      splits.head mustEqual new Text("00")
      splits.last mustEqual new Text("99")
    }

    "create a schema with custom record splitting options with talbe sharing on" in {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;table.splitter.class=" +
        s"${classOf[DigitSplitter].getName},table.splitter.options='fmt:%02d,min:0,max:99'"
      val sft = SimpleFeatureTypes.createType("customsplit2", spec)
      sft.setTableSharing(true)

      import scala.collection.JavaConversions._
      val prevsplits = ImmutableSet.copyOf(ds.connector.tableOperations().listSplits("AccumuloDataStoreTest_records").toIterable)
      ds.createSchema(sft)
      val recTable = ds.getTableName(sft.getTypeName, RecordTable)
      val afterSplits = ds.connector.tableOperations().listSplits(recTable)

      object TextOrdering extends Ordering[Text] {
        def compare(a: Text, b: Text) = a.compareTo(b)
      }
      val newSplits = (afterSplits.toSet -- prevsplits.toSet).toList.sorted(TextOrdering)
      val prefix = ds.getSchema(sft.getTypeName).getTableSharingPrefix
      newSplits.length mustEqual 100
      newSplits.head mustEqual new Text(s"${prefix}00")
      newSplits.last mustEqual new Text(s"${prefix}99")
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
        val qpt = dst.getQueryPlan(query)
        qpt must haveSize(1)
        qpt.head.table mustEqual dst.getTableName(sftName, Z3Table)
        qpt.head.numThreads mustEqual numThreads
      }

      forall(Seq(1, 5, 8, 20, 100))(testThreads)

      // check default
      val qpt = ds.getQueryPlan(query)
      qpt must haveSize(1)
      qpt.head.table mustEqual ds.getTableName(sftName, Z3Table)
      qpt.head.numThreads mustEqual 8
    }

    "allow users to call explainQuery" in {
      val sftName = "explainQueryTest"
      createSchema(sftName)
      val query = new Query(sftName, Filter.INCLUDE)
      val fr = ds.getFeatureReader(sftName)
      fr must not beNull
      val out = new ExplainString
      ds.explainQuery(new Query(sftName, Filter.INCLUDE), out)
      val explain = out.toString()
      explain must startWith(s"Planning '$sftName'")
    }

    "allow secondary attribute indexes" >> {
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

      // indexed attribute
      val q1 = ff.equals(ff.property("name"), ff.literal("one"))
      val fr = ds.getFeatureReader(sftName, new Query(sftName, q1))
      val results = SelfClosingIterator(fr).toList
      results must haveLength(1)
      results.head.getAttribute("name") mustEqual "one"

      // non-indexed attributes
      val q2 = ff.equals(ff.property("numattr"), ff.literal(2))
      val fr2 = ds.getFeatureReader(sftName, new Query(sftName, q2))
      val results2 = SelfClosingIterator(fr2).toList
      results2 must haveLength(1)
      results2.head.getAttribute("numattr") mustEqual 2
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
      val sft = SimpleFeatureTypes.createType(sftName, s"name:String:index=true,dtg:Date,*geom:Point:srid=4326")
      sft.setTableSharing(false)
      ds.createSchema(sft)

      // encode groups of 2 hex chars since we are doing multibyte chars
      def enc(s: String): String = Hex.encodeHex(s.getBytes("UTF8")).grouped(2)
        .map{ c => "_" + c(0) + c(1) }.mkString.toLowerCase

      // three byte UTF8 chars result in 9 char string
      enc("你") must haveLength(9)
      enc("好") must haveLength(9)

      val encodedSFT = "nihao" + enc("你") + enc("好")
      encodedSFT mustEqual GeoMesaTable.hexEncodeNonAlphaNumeric(sftName)

      SpatioTemporalTable.formatTableName(defaultTable, sft) mustEqual
          s"${defaultTable}_${encodedSFT}_st_idx"
      RecordTable.formatTableName(defaultTable, sft) mustEqual
          s"${defaultTable}_${encodedSFT}_records"
      AttributeTable.formatTableName(defaultTable, sft) mustEqual
          s"${defaultTable}_${encodedSFT}_attr_idx"

      val c = ds.connector

      c.tableOperations().exists(defaultTable) must beTrue
      c.tableOperations().exists(s"${defaultTable}_${encodedSFT}_st_idx") must beTrue
      c.tableOperations().exists(s"${defaultTable}_${encodedSFT}_records") must beTrue
      c.tableOperations().exists(s"${defaultTable}_${encodedSFT}_attr_idx") must beTrue
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
        ds.explainQuery(query, out)
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
        ds.explainQuery(query, out)
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
        ds.explainQuery(query, o)
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
        ds.explainQuery(query, o)
        o.toString()
      }
      ds.removeSchema(sftName)
      explain.split("\n").map(_.trim).filter(_.startsWith("Strategy filter:")).toSeq mustEqual
          Seq("Strategy filter: RECORD[INCLUDE][None]")
    }

    "create key plan that does not use STII when given something larger than the Whole World bbox" in {
      val sftName = "explainLargeBBOXTest3"
      val sft3 = createSchema(sftName)
      val filter = CQL.toFilter("bbox(geom, -190, -100, 190, 100)")
      val query = new Query(sftName, filter, Array("geom"))
      val explain = {
        val o = new ExplainString
        ds.explainQuery(query, o)
        o.toString()
      }
      ds.removeSchema(sftName)
      explain.split("\n").map(_.trim).filter(_.startsWith("Strategy filter:")).toSeq mustEqual
          Seq("Strategy filter: RECORD[INCLUDE][None]")
    }

    "create key plan that does not use STII when given an or'd geometry query with redundant bbox" in {
      // Todo: https://geomesa.atlassian.net/browse/GEOMESA-785
      val sftName = "explainLargeBBOXTest4"
      val sft3 = createSchema(sftName)
      val filter = CQL.toFilter("bbox(geom, -180, -90, 180, 90) OR bbox(geom, -10, -10, 10, 10)")
      val query = new Query(sftName, filter, Array("geom"))
      val explain = {
        val o = new ExplainString
        ds.explainQuery(query, o)
        o.toString()
      }
      ds.removeSchema(sftName)
      explain.split("\n").filter(_.startsWith("Filter:")) mustEqual Seq("Filter: primary filter: INCLUDE, secondary filter: None")
    }.pendingUntilFixed("Fixed query planner to deal with OR'd redundant geom with whole world")

    "create key plan that does not use STII when given two bboxes that when unioned are the whole world" in {
      // Todo: https://geomesa.atlassian.net/browse/GEOMESA-785
      val sftName = "explainWhatIsLogicallyTheWholeWorldTest1"
      val sft4 = createSchema(sftName)
      val filter = CQL.toFilter("bbox(geom, -180, -90, 0, 90) OR bbox(geom, 0, -90, 180, 90)")
      val query = new Query(sftName, filter, Array("geom"))
      val explain = {
        val o = new ExplainString
        ds.explainQuery(query, o)
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
        ds.explainQuery(query, o)
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

    "delete all associated tables" >> {
      val catalog = "AccumuloDataStoreDeleteTest"
      val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))
      val ds = DataStoreFinder.getDataStore(Map(
        "connector" -> connector,
        // note the table needs to be different to prevent testing errors
        "tableName" -> catalog)).asInstanceOf[AccumuloDataStore]
      val sft = SimpleFeatureTypes.createType(catalog, "name:String:index=true,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)
      val tables = GeoMesaTable.getTableNames(sft, ds) ++ Seq(catalog)
      tables must haveSize(5)
      connector.tableOperations().list().toSeq must containAllOf(tables)
      ds.delete()
      connector.tableOperations().list().toSeq must not(containAnyOf(tables))
    }

    "query on bbox and unbounded temporal" >> {
      val sftName = "unboundedTemporal"
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326"
      val sft = createSchema(sftName, spec)

      val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
      val features = (0 until 6).map { i =>
        builder.set("geom", WKTUtils.read(s"POINT(45.0 4$i.0)"))
        builder.set("dtg", s"2012-01-02T05:0$i:07.000Z")
        builder.set("name", i.toString)
        val sf = builder.buildFeature(i.toString)
        sf.getUserData.update(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf
      }

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
      fs.addFeatures(new ListFeatureCollection(sft, features))

      val query = new Query(sftName,
        ECQL.toFilter("BBOX(geom, 40.0, 40.0, 50.0, 44.5) AND dtg after 2012-01-02T05:02:00.000Z"))
      val reader = ds.getFeatureReader(sftName, query)

      val read = SelfClosingIterator(reader).toList

      // verify that all the attributes came back
      read must haveSize(3)
      read.map(_.getID).sorted mustEqual Seq("2", "3", "4")
    }

    "create tables with an accumulo namespace" >> {
      val table = "test.AccumuloDataStoreNamespaceTest"
      val params = Map("connector" -> ds.connector, "tableName" -> table)
      if (AccumuloVersion.accumuloVersion == AccumuloVersion.V15) {
        DataStoreFinder.getDataStore(params) must throwAn[IllegalArgumentException]
      } else {
        val dsWithNs = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
        val nsOps = classOf[Connector].getMethod("namespaceOperations").invoke(dsWithNs.connector)
        AccumuloVersion.nameSpaceExists(nsOps, nsOps.getClass, "test") must beTrue
      }
    }
  }
}
