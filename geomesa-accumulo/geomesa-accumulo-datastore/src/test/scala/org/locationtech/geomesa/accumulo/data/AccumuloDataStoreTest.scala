/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data._
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.Z2Iterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.index.NamedIndex
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.locationtech.geomesa.utils.text.{StringSerialization, WKTUtils}
import org.locationtech.jts.geom.{Geometry, Point}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.IOException
import java.util.Date

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreTest extends Specification with TestWithMultipleSfts {

  import scala.collection.JavaConverters._

  val defaultSpec = "name:String,geom:Point:srid=4326,dtg:Date"

  lazy val defaultSft = createNewSchema(defaultSpec)
  lazy val defaultTypeName = defaultSft.getTypeName
  val defaultGeom = WKTUtils.read("POINT(45.0 49.0)")

  step {
    addFeature(defaultPoint(defaultSft))
    addFeature(defaultPoint(defaultSft, id = "f2"))
  }

  def defaultPoint(sft: SimpleFeatureType,
                   id: String = "f1",
                   name: String = "testType",
                   point: String = "POINT(45.0 49.0)"): SimpleFeature = {
    new ScalaSimpleFeature(sft, id, Array(name, WKTUtils.read(point), new Date(100000)))
  }

  "AccumuloDataStore" should {
    "create a store" in {
      ds must not(beNull)
    }

    "create a store with old parameters" in {
      val params = dsParams.map {
        case (AccumuloDataStoreParams.UserParam.key, value)         => "user"       -> value
        case (AccumuloDataStoreParams.PasswordParam.key, value)     => "password"   -> value
        case (AccumuloDataStoreParams.InstanceNameParam.key, value) => "instanceId" -> value
        case (AccumuloDataStoreParams.ZookeepersParam.key, value)   => "zookeepers" -> value
        case (AccumuloDataStoreParams.CatalogParam.key, value)      => "tableName"  -> value
        case kv => kv
      }
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)
      try {
        ds must beAnInstanceOf[AccumuloDataStore]
        ds.asInstanceOf[AccumuloDataStore].config.catalog mustEqual catalog
      } finally {
        ds.dispose()
      }
    }

    "create a schema" in {
      ds.getSchema(defaultSft.getTypeName) mustEqual defaultSft
    }

    "create a schema with or without logical time" in {
      val logical = createNewSchema(defaultSpec)
      val millis = createNewSchema(defaultSpec + ";geomesa.logical.time=false")

      Seq(logical, millis).foreach(sft => addFeature(defaultPoint(sft)))
      val timestamp = System.currentTimeMillis()

      foreach(ds.getAllIndexTableNames(logical.getTypeName)) { index =>
        foreach(ds.connector.createScanner(index, new Authorizations).asScala) { entry =>
          entry.getKey.getTimestamp mustEqual 1L // logical time - incrementing counter
        }
      }
      foreach(ds.getAllIndexTableNames(millis.getTypeName)) { index =>
        foreach(ds.connector.createScanner(index, new Authorizations).asScala) { entry =>
          entry.getKey.getTimestamp must beCloseTo(timestamp, 10000L) // millis time - sys time
        }
      }
    }

    "prevent opening connections after dispose is called" in {
      val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]
      ds.dispose()
      ds.createSchema(SimpleFeatureTypes.createType("dispose", defaultSpec)) must throwAn[IllegalStateException]
    }

    "escape a ~ in the feature name" in {
      val sft = SimpleFeatureTypes.createType("name~name", "name:String,geom:Point:srid=4326")
      ds.createSchema(sft)
      ds.getSchema("name~name") must not(beNull)
      ds.getSchema("name~name").getTypeName mustEqual "name~name"
      ds.getTypeNames.contains("name~name") must beTrue
    }

    "create and retrieve a schema without a geometry" in {

      val sft = createNewSchema("name:String")

      val retrievedSft = ds.getSchema(sft.getTypeName)

      retrievedSft must not(beNull)
      retrievedSft.getAttributeCount mustEqual 1

      val f = new ScalaSimpleFeature(sft, "fid1", Array("my name"))
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      val f2 = new ScalaSimpleFeature(sft, "replaceme", Array("my other name"))

      val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]

      val fc = new DefaultFeatureCollection()
      fc.add(f)
      fc.add(f2)
      val ids = fs.addFeatures(fc)
      ids.asScala.map(_.getID).find(_ != "fid1").foreach(f2.setId)

      SelfClosingIterator(fs.getFeatures(Filter.INCLUDE).features).toList must containTheSameElementsAs(List(f, f2))
      SelfClosingIterator(fs.getFeatures(ECQL.toFilter("IN('fid1')")).features).toList mustEqual List(f)
      SelfClosingIterator(fs.getFeatures(ECQL.toFilter("name = 'my name'")).features).toList mustEqual List(f)
      SelfClosingIterator(fs.getFeatures(ECQL.toFilter("name = 'my other name'")).features).toList mustEqual List(f2)
      SelfClosingIterator(fs.getFeatures(ECQL.toFilter("name = 'false'")).features).toList must beEmpty

      ds.removeSchema(sft.getTypeName)
      ds.getSchema(sft.getTypeName) must beNull
    }

    "return NULL when a feature name does not exist" in {
      ds.getSchema("testTypeThatDoesNotExist") must beNull
    }

    "return type names" in {
      ds.getTypeNames.toSeq must contain(defaultSft.getTypeName)
    }

    "provide ability to write using the feature source and read what it wrote" in {

      // compose a CQL query that uses a reasonably-sized polygon for searching
      val cqlFilter = CQL.toFilter(s"BBOX(geom, 44.9,48.9,45.1,49.1)")
      val query = new Query(defaultTypeName, cqlFilter)

      // Let's read out what we wrote.
      val results = ds.getFeatureSource(defaultTypeName).getFeatures(query)
      val features = SelfClosingIterator(results.features).toList

      results.getSchema mustEqual defaultSft
      features must haveLength(2)
      forall(features)(_.getDefaultGeometry mustEqual defaultGeom)
    }

    "create a schema with custom record splitting options with table sharing off" in {
      val spec = "name:String,dtg:Date,*geom:Point:srid=4326;table.splitter.options='id.pattern:[a-z][0-9]'"
      val sft = SimpleFeatureTypes.createType("customsplit", spec)
      ds.createSchema(sft)
      val recTables = ds.manager.indices(sft).find(_.name == IdIndex.name).toSeq.flatMap(_.getTableNames())
      recTables must not(beEmpty)
      foreach(recTables) { recTable =>
        val splits = ds.connector.tableOperations().listSplits(recTable)
        // note: first split is dropped, which creates 259 splits but 260 regions
        splits.size() mustEqual 259
        splits.asScala.head mustEqual new Text("a1")
        splits.asScala.last mustEqual new Text("z9")
      }
    }

    "Prevent mixed geometries in spec" in {
      "throw an exception if geometry is specified" >> {
        createNewSchema("name:String,dtg:Date,*geom:Geometry:srid=4326") must throwA[IllegalArgumentException]
      }
      "allow for override" >> {
        val sft = createNewSchema("name:String,dtg:Date,*geom:Geometry:srid=4326;geomesa.mixed.geometries=true")
        sft.getGeometryDescriptor.getType.getBinding mustEqual classOf[Geometry]
      }
    }

    "Prevent reserved words in spec" in {
      "throw an exception if reserved words are found" >> {
        createNewSchema("name:String,dtg:Date,*Point:Point:srid=4326") must throwAn[IllegalArgumentException]
      }
      "allow for override" >> {
        val sft = createNewSchema("name:String,dtg:Date,*Point:Point:srid=4326;override.reserved.words=true")
        sft.getGeometryDescriptor.getType.getBinding mustEqual classOf[Point]
      }
    }

    "Prevent join indices on default date" in {
      "throw an exception if join index is found" >> {
        createNewSchema("name:String,dtg:Date:index=join,*geom:Point:srid=4326") must throwAn[IllegalArgumentException]
        createNewSchema("name:String,dtg:Date:index=join,*geom:Point:srid=4326") must throwAn[IllegalArgumentException]
      }
      "allow for full indices" >> {
        val sft = createNewSchema("name:String,dtg:Date:index=full,*geom:Point:srid=4326")
        ds.manager.indices(sft).exists(i => i.name == AttributeIndex.name && i.attributes.headOption.contains("dtg")) must beTrue
      }
      "allow for override" >> {
        val sft = createNewSchema("name:String,dtg:Date:index=join,*geom:Point:srid=4326;override.index.dtg.join=true")
        ds.manager.indices(sft).exists(i => i.name == JoinIndex.name && i.attributes.headOption.contains("dtg")) must beTrue
      }
    }

    "allow for a configurable number of threads in z3 queries" in {
      val param = AccumuloDataStoreParams.QueryThreadsParam.getName
      val query = new Query(defaultTypeName, ECQL.toFilter("bbox(geom,-75,-75,-60,-60) AND " +
          "dtg DURING 2010-05-07T00:00:00.000Z/2010-05-08T00:00:00.000Z"))

      def testThreads(numThreads: Option[Int]) = {
        val params = dsParams ++ numThreads.map(n => Map(param -> n)).getOrElse(Map.empty)
        val dst = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
        val z3Tables = dst.manager.indices(defaultSft).filter(_.name == Z3Index.name).flatMap(_.getTableNames())
        forall(dst.getQueryPlan(query)) { qpt =>
          qpt.tables mustEqual z3Tables
          qpt.numThreads mustEqual numThreads.getOrElse(8) // 8 is the default
        }
      }

      forall(Seq(Some(1), Some(5), Some(8), Some(20), Some(100), None))(testThreads)
    }

    "allow users to call explainQuery" in {
      val out = new ExplainString
      ds.getQueryPlan(new Query(defaultTypeName, Filter.INCLUDE), explainer = out)
      val explain = out.toString()
      explain must startWith(s"Planning '$defaultTypeName'")
    }

    "return a list of all accumulo tables associated with a schema" in {
      val indices = ds.manager.indices(defaultSft).flatMap(_.getTableNames())
      val expected = Seq(catalog, s"${catalog}_stats", s"${catalog}_queries") ++ indices
      ds.getAllTableNames(defaultTypeName) must containTheSameElementsAs(expected)
    }

    "allow secondary attribute indexes" >> {
      val sft = createNewSchema("name:String:index=join,numattr:Integer,dtg:Date,*geom:Point:srid=4326")
      val sftName = sft.getTypeName

      "create all appropriate tables" >> {
        val tables = ds.getAllIndexTableNames(sft.getTypeName)
        tables must haveLength(4)
        forall(Seq(Z2Index, Z3Index, AttributeIndex))(t => tables must contain(endWith(t.name)))
        forall(tables)(t => ds.connector.tableOperations.exists(t) must beTrue)
      }

      val pt = WKTUtils.read("POINT (0 0)")
      val one = ScalaSimpleFeature.create(sft, "1", "one", new Integer(1), new Date(), pt)
      val two = ScalaSimpleFeature.create(sft, "2", "two", new Integer(2), new Date(), pt)

      val fs = ds.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]
      fs.addFeatures(DataUtilities.collection(java.util.Arrays.asList[SimpleFeature](one, two)))

      // indexed attribute
      val q1 = ECQL.toFilter("name = 'one'")
      val fr = ds.getFeatureReader(new Query(sftName, q1), Transaction.AUTO_COMMIT)
      val results = SelfClosingIterator(fr).toList
      results must haveLength(1)
      results.head.getAttribute("name") mustEqual "one"

      // non-indexed attributes
      val q2 = ECQL.toFilter("numattr = 2")
      val fr2 = ds.getFeatureReader(new Query(sftName, q2), Transaction.AUTO_COMMIT)
      val results2 = SelfClosingIterator(fr2).toList
      results2 must haveLength(1)
      results2.head.getAttribute("numattr") mustEqual 2
    }

    "support attribute indices on timestamps" in {
      val sft = createNewSchema("dtg:Date,ts:Timestamp:index=true,*geom:Point:srid=4326")
      val f = ScalaSimpleFeature.create(sft, "0", "2018-01-01T00:00:00.000Z", "2018-01-01T00:00:00.000Z", "POINT (45 55)")
      addFeature(f)
      val query = new Query(sft.getTypeName, ECQL.toFilter("ts = '2018-01-01T00:00:00.000Z'"))
      SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList mustEqual Seq(f)
    }

    "hex encode multibyte chars as multiple underscore + hex" in {
      // accumulo supports only alphanum + underscore aka ^\\w+$
      // this should end up hex encoded
      val sftName = "nihao你好"
      val sft = SimpleFeatureTypes.createType(sftName, s"name:String:index=join,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)

      // encode groups of 2 hex chars since we are doing multibyte chars
      def enc(s: String): String = Hex.encodeHex(s.getBytes("UTF8")).grouped(2)
        .map{ c => "_" + c(0) + c(1) }.mkString.toLowerCase

      // three byte UTF8 chars result in 9 char string
      enc("你") must haveLength(9)
      enc("好") must haveLength(9)

      val encodedSFT = "nihao" + enc("你") + enc("好")
      encodedSFT mustEqual StringSerialization.alphaNumericSafeString(sftName)

      forall(ds.manager.indices(sft)) { index =>
        forall(index.getTableNames()) {
          _ must startWith(s"${ds.config.catalog}_${encodedSFT}_${StringSerialization.alphaNumericSafeString(index.name)}")
        }
      }

      val c = ds.connector

      c.tableOperations().exists(ds.config.catalog) must beTrue
      forall(ds.manager.indices(sft).flatMap(_.getTableNames())) { table =>
        c.tableOperations().exists(table) must beTrue
      }
    }

    "update metadata for indexed attributes" in {
      val sft = SimpleFeatureTypes.mutable(createNewSchema("name:String,dtg:Date,*geom:Point:srid=4326"))
      sft.getIndices.map(_.name) must containTheSameElementsAs(Seq(Z3Index, Z2Index, IdIndex).map(_.name))
      sft.getAttributeDescriptors.get(0).getUserData.put(AttributeOptions.OptIndex, IndexCoverage.JOIN.toString)
      ds.updateSchema(sft.getTypeName, sft)
      ds.getSchema(sft.getTypeName).getIndices.map(_.name) must
          containTheSameElementsAs(Seq(Z3Index, Z2Index, IdIndex, JoinIndex).map(_.name))
    }

    "prevent changing schema types" in {
      val originalSchema = "name:String,dtg:Date,*geom:Point:srid=4326"
      val sftName = createNewSchema(originalSchema).getTypeName

      def modify(spec: String): Unit = {
        val modified = SimpleFeatureTypes.createType(sftName, spec)
        modified.getUserData.putAll(ds.getSchema(sftName).getUserData)
        ds.updateSchema(sftName, modified)
      }

      // "prevent changing default geometry" >> {
      modify("name:String,dtg:Date,geom:Point:srid=4326,*geom2:Point:srid=4326") must throwAn[UnsupportedOperationException]
      SimpleFeatureTypes.encodeType(ds.getSchema(sftName)) mustEqual originalSchema
      // "prevent changing attribute order" >> {
      modify("dtg:Date,name:String,*geom:Point:srid=4326") must throwA[UnsupportedOperationException]
      SimpleFeatureTypes.encodeType(ds.getSchema(sftName)) mustEqual originalSchema
      // "prevent removing attributes" >> {
      modify("dtg:Date,*geom:Point:srid=4326") must throwA[UnsupportedOperationException]
      SimpleFeatureTypes.encodeType(ds.getSchema(sftName)) mustEqual originalSchema
      // "allow adding attributes" >> {
      // note: we actually modify the schema here so this check is last
      val newSchema = "name:String,dtg:Date,*geom:Point:srid=4326,newField:String"
      modify(newSchema)
      SimpleFeatureTypes.encodeType(ds.getSchema(sftName)) mustEqual newSchema
    }

    "Provide a feature update implementation" in {
      val sft = createNewSchema("name:String,dtg:Date,*geom:Point:srid=4326")
      val sftName = sft.getTypeName

      addFeatures((0 until 6).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setAttributes(Array[AnyRef](i.toString, "2012-01-02T05:06:07.000Z", "POINT(45.0 45.0)"))
        sf
      })

      val filter = ECQL.toFilter("IN ('2')")
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
      updated.getID mustEqual "2"
      updated.getAttribute("name") mustEqual "2-updated"
    }

    "Project to date/geom" in {
      val sft = createNewSchema("name:String,dtg:Date,*geom:Point:srid=4326,attr2:String")
      val sftName = sft.getTypeName

      val features = (0 until 6).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setAttributes(Array[AnyRef](i.toString, s"2012-01-02T05:0$i:07.000Z", s"POINT(45.0 4$i.0)", s"2-$i"))
        sf
      }
      addFeatures(features)

      val baseTime = features(0).getAttribute("dtg").asInstanceOf[Date].getTime

      val query = new Query(sftName, ECQL.toFilter("BBOX(geom, 40.0, 40.0, 50.0, 50.0)"), "geom", "dtg")
      val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)

      val read = SelfClosingIterator(reader).toList

      // verify that all the attributes came back
      read must haveSize(6)
      read.sortBy(_.getAttribute("dtg").toString).zipWithIndex.foreach { case (sf, i) =>
        sf.getAttributeCount mustEqual 2
        sf.getAttribute("name") must beNull
        sf.getAttribute("geom") mustEqual WKTUtils.read(s"POINT(45.0 4$i.0)")
        sf.getAttribute("dtg").asInstanceOf[Date].getTime mustEqual baseTime + i * 60000
      }
      ok
    }

    "create query plan that uses the Z2 iterator with simple bbox" in {
      val query = new Query(defaultTypeName, ECQL.toFilter("bbox(geom, -100, -45, 100, 45)"))
      val plans = ds.getQueryPlan(query)
      forall(plans) { plan =>
        plan.iterators.map(_.getIteratorClass) must containTheSameElementsAs(Seq(classOf[Z2Iterator].getName))
      }
    }

    "create key plan that does not use any iterators when given the Whole World bbox" in {
      val query = new Query(defaultTypeName, ECQL.toFilter("bbox(geom, -180, -90, 180, 90)"))
      val plans = ds.getQueryPlan(query)
      plans must haveLength(1)
      plans.head.iterators must beEmpty
    }

    "create key plan that does not use STII when given something larger than the Whole World bbox" in {
      val query = new Query(defaultTypeName, ECQL.toFilter("bbox(geom, -190, -100, 190, 100)"), "geom")
      val plan = ds.getQueryPlan(query)
      plan must haveLength(1)
      plan.head.filter.index.name mustEqual Z3Index.name
      plan.head.filter.filter must beNone
    }

    "create key plan that does not use STII when given an or'd geometry query with redundant bbox" in {
      val query = new Query(defaultTypeName, ECQL.toFilter("bbox(geom, -180, -90, 180, 90) OR bbox(geom, -10, -10, 10, 10)"))
      val plans = ds.getQueryPlan(query)
      plans must haveLength(1)
      plans.head.iterators must beEmpty
    }

    "create key plan that does not use STII when given two bboxes that when unioned are the whole world" in {
      // Todo: https://geomesa.atlassian.net/browse/GEOMESA-785
      val query = new Query(defaultTypeName, ECQL.toFilter("bbox(geom, -180, -90, 0, 90) OR bbox(geom, 0, -90, 180, 90)"))
      val plans = ds.getQueryPlan(query)
      plans must haveLength(1)
      plans.head.iterators must beEmpty
    }.pendingUntilFixed("Fixed query planner to deal with OR'd whole world geometry")

    "transform index value data correctly" in {
      import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

      val sft = createNewSchema("trackId:String:index-value=true,label:String:index-value=true," +
          "extraValue:String,score:Double:index-value=true,dtg:Date,geom:Point:srid=4326")
      val sftName = sft.getTypeName

      addFeatures((0 until 5).map { i =>
        val sf = new ScalaSimpleFeature(sft, s"f$i")
        sf.setAttributes(Array[AnyRef](s"trk$i", s"label$i", "extra", s"$i", s"2014-01-01T0$i:00:00.000Z", s"POINT(5$i 50)"))
        sf
      })

      def query(filter: Filter, transforms: String*): List[SimpleFeature] = {
        var query = new Query(sftName, filter, transforms: _*)
        SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features).toList.sortBy(_.getID)
      }

      // "with out of order attributes" >> {
      val features0 = query(ECQL.toFilter("bbox(geom,49,49,60,60)"), "geom", "dtg", "label")
      features0 must haveSize(5)
      foreach(0 until 5) { i =>
        features0(i).getID mustEqual s"f$i"
        features0(i).getAttributeCount mustEqual 3
        features0(i).getAttribute("label") mustEqual s"label$i"
        features0(i).getAttribute("dtg") mustEqual java.util.Date.from(java.time.LocalDateTime.parse(s"2014-01-01T0$i:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
        features0(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
      }

      // "with only date and geom" >> {
      val features1 = query(ECQL.toFilter("bbox(geom,49,49,60,60)"), "geom", "dtg")
      features1 must haveSize(5)
      foreach(0 until 5) { i =>
        features1(i).getID mustEqual s"f$i"
        features1(i).getAttributeCount mustEqual 2
        features1(i).getAttribute("dtg") mustEqual java.util.Date.from(java.time.LocalDateTime.parse(s"2014-01-01T0$i:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
        features1(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
      }

      // "with all attributes" >> {
      val features2 = query(ECQL.toFilter("bbox(geom,49,49,60,60)"), "geom", "dtg", "label", "score", "trackId")
      features2 must haveSize(5)
      foreach(0 until 5) { i =>
        features2(i).getID mustEqual s"f$i"
        features2(i).getAttributeCount mustEqual 5
        features2(i).getAttribute("label") mustEqual s"label$i"
        features2(i).getAttribute("trackId") mustEqual s"trk$i"
        features2(i).getAttribute("score") mustEqual i.toDouble
        features2(i).getAttribute("dtg") mustEqual java.util.Date.from(java.time.LocalDateTime.parse(s"2014-01-01T0$i:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
        features2(i).getAttribute("geom") mustEqual WKTUtils.read(s"POINT(5$i 50)")
      }
    }

    "delete all associated tables" in {
      val catalog = "AccumuloDataStoreDeleteAllTablesTest"
      // note the table needs to be different to prevent testing errors
      val ds = DataStoreFinder.getDataStore((dsParams ++ Map(AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava).asInstanceOf[AccumuloDataStore]
      val sft = SimpleFeatureTypes.createType(catalog, "name:String:index=join,dtg:Date,*geom:Point:srid=4326")
      ds.createSchema(sft)
      val tables = ds.getAllIndexTableNames(sft.getTypeName) ++ Seq(catalog)
      tables must haveSize(5)
      ds.connector.tableOperations().list().asScala.toSeq must containAllOf(tables)
      ds.delete()
      ds.connector.tableOperations().list().asScala.toSeq must not(containAnyOf(tables))
    }

    "query on bbox and unbounded temporal" in {
      val sft = createNewSchema("name:String,dtg:Date,*geom:Point:srid=4326")

      addFeatures((0 until 6).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setAttributes(Array[AnyRef](i.toString, s"2012-01-02T05:0$i:07.000Z", s"POINT(45.0 4$i.0)"))
        sf
      })

      val query = new Query(sft.getTypeName,
        ECQL.toFilter("BBOX(geom, 40.0, 40.0, 50.0, 44.5) AND dtg after 2012-01-02T05:02:00.000Z"))
      val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)

      val read = SelfClosingIterator(reader).toList

      // verify that all the attributes came back
      read must haveSize(3)
      read.map(_.getID) must containAllOf(Seq("2", "3", "4"))
    }

    "create tables with an accumulo namespace" in {
      val table = "test.AccumuloDataStoreNamespaceTest"
      val params = dsParams ++ Map(AccumuloDataStoreParams.CatalogParam.key -> table)
      val dsWithNs = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
      val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")
      dsWithNs.createSchema(sft)
      dsWithNs.connector.namespaceOperations().exists("test") must beTrue
    }

    "only create catalog table when necessary" in {
      val table = "AccumuloDataStoreTableTest"
      val params = dsParams ++ Map(AccumuloDataStoreParams.CatalogParam.key -> table)
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
      ds must not(beNull)
      def exists = ds.connector.tableOperations().exists(table)
      exists must beFalse
      ds.getTypeNames must beEmpty
      exists must beFalse
      ds.getSchema("test") must beNull
      exists must beFalse
      ds.getFeatureReader(new Query("test"), Transaction.AUTO_COMMIT) must throwAn[IOException]
      exists must beFalse
      ds.createSchema(SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326"))
      exists must beTrue
      ds.getSchema("test") must not(beNull)
    }

    "create tables with block cache enabled/disabled" in {
      foreach(Seq(",geomesa.table.partition=time", "")) { partitioned =>
        val sft = createNewSchema(s"name:String:index=true,dtg:Date,*geom:Point:srid=4326;table.cache.enabled='z3,attr'$partitioned")
        addFeatures((0 until 6).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttributes(Array[AnyRef](i.toString, s"2012-01-02T05:0$i:07.000Z", s"POINT(45.0 4$i.0)"))
          sf
        })
        val indices = ds.manager.indices(sft)
        def getBlockCacheConfig(i: NamedIndex): String = {
          val index = indices.find(_.name == i.name).orNull
          index must not(beNull)
          val tables = index.getTableNames()
          tables must haveLength(1)
          ds.connector.tableOperations().getProperties(tables.head).asScala
            .find(_.getKey == Property.TABLE_BLOCKCACHE_ENABLED.getKey).map(_.getValue).orNull
        }
        foreach(Seq(Z3Index, AttributeIndex)) { i =>
          getBlockCacheConfig(i) mustEqual "true"
        }
        foreach(Seq(Z2Index, IdIndex)) { i =>
          getBlockCacheConfig(i) mustEqual "false"
        }
      }
    }

    "create index tables with a different prefix than the catalog table" in {
      val prefix = s"custom.${catalog.replaceFirst(".*\\.", "")}"
      foreach(Seq(",geomesa.table.partition=time", "")) { partitioned =>
        val userData = s"index.table.prefix='$prefix',index.table.prefix.z3='z3$prefix'$partitioned"
        val sft = createNewSchema(s"name:String:index=true,dtg:Date,*geom:Point:srid=4326;$userData")
        addFeatures((0 until 6).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttributes(Array[AnyRef](i.toString, s"2012-01-02T05:0$i:07.000Z", s"POINT(45.0 4$i.0)"))
          sf
        })
        foreach(ds.manager.indices(sft)) { index =>
          val p = if (index.name == Z3Index.name) { s"z3$prefix" } else { prefix }
          foreach(index.getTableNames())(_ must startWith(s"${p}_${sft.getTypeName}_"))
        }
      }
    }
  }
}
