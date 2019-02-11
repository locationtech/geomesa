/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.cql2.CQLException
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{BatchScanPlan, JoinPlan}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.planning.FilterSplitter
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.matcher.Matcher
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AttributeIndexStrategyTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "name:String:index=full,age:Integer:index=join,count:Long:index=join," +
      "weight:Double:index=join,height:Float:index=join,admin:Boolean:index=join," +
      "*geom:Point:srid=4326,dtg:Date,indexedDtg:Date:index=join,fingers:List[String]:index=join," +
      "toes:List[Double]:index=join,track:String,geom2:Point:srid=4326;geomesa.indexes.enabled='attr,id'"

  val aliceGeom   = WKTUtils.read("POINT(45.0 49.0)")
  val billGeom    = WKTUtils.read("POINT(46.0 49.0)")
  val bobGeom     = WKTUtils.read("POINT(47.0 49.0)")
  val charlesGeom = WKTUtils.read("POINT(48.0 49.0)")

  val aliceDate   = Converters.convert("2012-01-01T12:00:00.000Z", classOf[Date])
  val billDate    = Converters.convert("2013-01-01T12:00:00.000Z", classOf[Date])
  val bobDate     = Converters.convert("2014-01-01T12:00:00.000Z", classOf[Date])
  val charlesDate = Converters.convert("2014-01-01T12:30:00.000Z", classOf[Date])

  val aliceFingers   = List("index")
  val billFingers    = List("ring", "middle")
  val bobFingers     = List("index", "thumb", "pinkie")
  val charlesFingers = List("thumb", "ring", "index", "pinkie", "middle")

  val geom2 = WKTUtils.read("POINT(55.0 59.0)")

  val features = Seq(
    Array("alice",   20,   1, 5.0, 10.0F, true,  aliceGeom, aliceDate, aliceDate, aliceFingers, List(1.0), "track1", geom2),
    Array("bill",    21,   2, 6.0, 11.0F, false, billGeom, billDate, billDate, billFingers, List(1.0, 2.0), "track2", geom2),
    Array("bob",     30,   3, 6.0, 12.0F, false, bobGeom, bobDate, bobDate, bobFingers, List(3.0, 2.0, 5.0), "track1", geom2),
    Array("charles", null, 4, 7.0, 12.0F, false, charlesGeom, charlesDate, charlesDate, charlesFingers, List(), "track1", geom2)
  ).map { entry =>
    val feature = new ScalaSimpleFeature(sft, entry.head.toString)
    feature.setAttributes(entry.asInstanceOf[Array[AnyRef]])
    feature
  }

  val shards = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getAttributeShards
  }

  step {
    addFeatures(features)
  }

  def execute(filter: String, explain: Explainer = ExplainNull, ranges: Option[Matcher[Int]] = None): List[String] = {
    val query = new Query(sftName, ECQL.toFilter(filter))
    forall(ds.getQueryPlan(query, explainer = explain)) { qp =>
      qp.filter.index.name must beOneOf(AttributeIndex.name, JoinIndex.name)
      forall(ranges)(_.test(qp.ranges.length))
    }
    val results = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features())
    results.map(_.getAttribute("name").toString).toList
  }

  def runQuery(query: Query, explain: Explainer = ExplainNull): Iterator[SimpleFeature] = {
    forall(ds.getQueryPlan(query, explainer = explain)) { qp =>
      qp.filter.index.name must beOneOf(AttributeIndex.name, JoinIndex.name)
    }
    SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features())
  }

  "AttributeIndexStrategy" should {
    "print values" in {
      skipped("used for debugging")
      ds.manager.indices(sft).foreach { index =>
        if (index.name == AttributeIndex.name || index.name == JoinIndex.name) {
          index.getTableNames().foreach { table =>
            println(table)
            WithClose(connector.createScanner(table, MockUserAuthorizations))(_.asScala.foreach(println))
          }
          println()
        }
      }
      success
    }

    "all attribute filters should be applied to SFFI" in {
      val filter = andFilters(Seq(ECQL.toFilter("name LIKE 'b%'"), ECQL.toFilter("count<27"), ECQL.toFilter("age<29")))
      val results = execute(ECQL.toCQL(filter))
      results must haveLength(1)
      results must contain ("bill")
    }

    "support bin queries with join queries" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[JoinPlan])
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq("bill", "bob", "charles").map(_.hashCode))
    }

    "support bin queries with join queries and transforms" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"), Array("dtg", "geom", "name")) // note: swap order
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_DTG, "dtg")
      query.getHints.put(BIN_GEOM, "geom")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[JoinPlan])
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq("bill", "bob", "charles").map(_.hashCode))
    }

    "support bin queries against index values" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      query.getHints.put(BIN_TRACK, "dtg")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[BatchScanPlan])
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq(billDate, bobDate, charlesDate).map(_.hashCode))
    }

    "support bin queries against full values" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("name>'amy'"))
      query.getHints.put(BIN_TRACK, "count")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[BatchScanPlan])
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq(2, 3, 4).map(_.hashCode))
    }

    "support bin queries against non-default geoms with index-value track" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      query.getHints.put(BIN_GEOM, "geom2")
      query.getHints.put(BIN_TRACK, "count")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[JoinPlan])
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq(2, 3, 4).map(_.hashCode))
      forall(bins.map(_.lat))(_ mustEqual 59f)
      forall(bins.map(_.lon))(_ mustEqual 55f)
    }

    "correctly query equals with spatio-temporal filter" in {
      // height filter matches bob and charles, st filters only match bob
      val stFilters = Seq(
        "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z",
        "bbox(geom, 46.5, 48, 47.5, 50) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z", // dtg + bbox
        "bbox(geom, 46.5, 48, 47.5, 50) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:45:00.000Z", // bbox only
        "bbox(geom, 46.9, 48.9, 48.1, 49.1) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z" // dtg only
      )
      forall(stFilters) { stFilter =>
        // expect z3 ranges with the attribute equals prefix
        val features = execute(s"height = 12.0 AND $stFilter", ranges = Some(beGreaterThan(shards)))
        features must haveLength(1)
        features must contain("bob")
      }
    }

    "correctly query lt with spatio-temporal filter" in {
      // height filter matches alice and bill, st filters only match alice
      val stFilters = Seq(
        "dtg DURING 2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z",
        "bbox(geom, 44.5, 48, 45.5, 50) AND dtg DURING 2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z", // dtg + bbox
        "bbox(geom, 44.5, 48, 45.5, 50) AND dtg DURING 2012-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z", // bbox only
        "bbox(geom, 44.5, 48.9, 46.5, 49.1) AND dtg DURING 2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z" // dtg only
      )
      forall(stFilters) { stFilter =>
        // expect z3 ranges to only inform the upper bound
        val features = execute(s"height < 12.0 AND $stFilter", ranges = Some(beEqualTo(shards)))
        features must haveLength(1)
        features must contain("alice")
      }
    }

    "correctly query lte with spatio-temporal filter" in {
      // height filter matches all, st filters only match bill and bob
      val stFilters = Seq(
        "dtg DURING 2013-01-01T00:00:00.000Z/2014-01-01T12:15:00.000Z",
        "bbox(geom, 45.5, 48, 47.5, 50) AND dtg DURING 2013-01-01T00:00:00.000Z/2014-01-01T12:15:00.000Z", // dtg + bbox
        "bbox(geom, 45.5, 48, 47.5, 50) AND dtg DURING 2011-01-01T00:00:00.000Z/2014-01-01T12:45:00.000Z", // bbox only
        "bbox(geom, 44.5, 48.9, 48.5, 49.1) AND dtg DURING 2013-01-01T00:00:00.000Z/2014-01-01T12:15:00.000Z" // dtg only
      )
      forall(stFilters) { stFilter =>
        // expect z3 ranges to only inform the upper bound
        val features = execute(s"height <= 12.0 AND $stFilter", ranges = Some(beEqualTo(shards)))
        features must haveLength(2)
        features must contain("bill", "bob")
      }
    }

    "correctly query gt with spatio-temporal filter" in {
      // height filter matches bob and charles, st filters only match bob
      val stFilters = Seq(
        "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z",
        "bbox(geom, 46.5, 48, 47.5, 50) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z", // dtg + bbox
        "bbox(geom, 46.5, 48, 47.5, 50) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:45:00.000Z", // bbox only
        "bbox(geom, 46.9, 48.9, 48.1, 49.1) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z" // dtg only
      )
      forall(stFilters) { stFilter =>
        // expect z3 ranges to only inform the lower bound
        val features = execute(s"height > 11.0 AND $stFilter", ranges = Some(beEqualTo(shards)))
        features must haveLength(1)
        features must contain("bob")
      }
    }

    "correctly query gte with spatio-temporal filter" in {
      // height filter matches bill, bob and charles, st filters only match bob
      val stFilters = Seq(
        "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z",
        "bbox(geom, 46.5, 48, 47.5, 50) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z", // dtg + bbox
        "bbox(geom, 46.5, 48, 47.5, 50) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:45:00.000Z", // bbox only
        "bbox(geom, 46.9, 48.9, 48.1, 49.1) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z" // dtg only
      )
      forall(stFilters) { stFilter =>
        // expect z3 ranges to only inform the lower bound
        val features = execute(s"height >= 11.0 AND $stFilter", ranges = Some(beEqualTo(shards)))
        features must haveLength(1)
        features must contain("bob")
      }
    }

    "correctly query between with spatio-temporal filter" in {
      // height filter matches bill, bob and charles, st filters only match bob
      val stFilters = Seq(
        "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z",
        "bbox(geom, 46.5, 48, 47.5, 50) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z", // dtg + bbox
        "bbox(geom, 46.5, 48, 47.5, 50) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:45:00.000Z", // bbox only
        "bbox(geom, 46.9, 48.9, 48.1, 49.1) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z" // dtg only
      )
      forall(stFilters) { stFilter =>
        // expect z3 ranges to only inform the end bounds
        val features = execute(s"height between 11.0 AND 12.0 AND $stFilter", ranges = Some(beEqualTo(shards)))
        features must haveLength(1)
        features must contain("bob")
      }
    }

    "handle functions" in {
      val filters = Seq (
        "strToUpperCase(name) = 'BILL'",
        "strCapitalize(name) = 'Bill'",
        "strConcat(name, 'foo') = 'billfoo'",
        "strIndexOf(name, 'ill') = 1",
        "strReplace(name, 'ill', 'all', false) = 'ball'",
        "strSubstring(name, 0, 2) = 'bi'",
        "strToLowerCase(name) = 'bill'",
        "strTrim(name) = 'bill'",
        "abs(age) = 21",
        "ceil(age) = 21",
        "floor(age) = 21",
        "'BILL' = strToUpperCase(name)",
        "strToUpperCase('bill') = strToUpperCase(name)",
        "strToUpperCase(name) = strToUpperCase('bill')",
        "name = strToLowerCase('bill')"
      )
      foreach(filters) { filter => execute(filter) mustEqual Seq("bill") }
    }

    "support sampling" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
    }

    "support sampling with cql" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a' AND track > 'track'"))
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
    }

    "support sampling with transformations" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"), Array("name", "geom"))
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling with cql and transformations" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a' AND track > 'track'"), Array("name", "geom"))
      query.getHints.put(SAMPLING, new java.lang.Float(.2f))
      val results = runQuery(query).toList
      results must haveLength(1)
      results.head.getAttributeCount mustEqual 2
    }

    "support sampling by thread" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      query.getHints.put(SAMPLE_BY, "track")
      val results = runQuery(query).toList
      results.length must beLessThan(4) // note: due to sharding and multiple ranges, we don't get exact sampling
      results.map(_.getAttribute("track")).distinct must containTheSameElementsAs(Seq("track1", "track2"))
    }

    "support sampling with bin queries" in {
      import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
      // important - id filters will create multiple ranges and cause multiple iterators to be created
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(BIN_TRACK, "name")
      query.getHints.put(BIN_BATCH_SIZE, 1000)
      query.getHints.put(SAMPLING, new java.lang.Float(.5f))
      // have to evaluate attributes before pulling into collection, as the same sf is reused
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(BinaryOutputEncoder.decode))
      bins must haveSize(2)
    }

    "support density queries against index values" in {
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      val envelope = new ReferencedEnvelope(30, 60, 30, 60, CRS_EPSG_4326)
      query.getHints.put(DENSITY_BBOX, envelope)
      query.getHints.put(DENSITY_HEIGHT, 600)
      query.getHints.put(DENSITY_WIDTH, 400)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[BatchScanPlan])
      val decode = DensityScan.decodeResult(envelope, 600, 400)
      val results = runQuery(query).flatMap(decode).toList
      results must containTheSameElementsAs(Seq((41.325,58.5375,1.0), (42.025,58.5375,1.0), (40.675,58.5375,1.0)))
    }

    "support density queries against index values with weight" in {
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      val envelope = new ReferencedEnvelope(30, 60, 30, 60, CRS_EPSG_4326)
      query.getHints.put(DENSITY_BBOX, envelope)
      query.getHints.put(DENSITY_HEIGHT, 600)
      query.getHints.put(DENSITY_WIDTH, 400)
      query.getHints.put(DENSITY_WEIGHT, "count")
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[BatchScanPlan])
      val decode = DensityScan.decodeResult(envelope, 600, 400)
      val results = runQuery(query).flatMap(decode).toList
      results must containTheSameElementsAs(Seq((41.325,58.5375,3.0), (42.025,58.5375,4.0), (40.675,58.5375,2.0)))
    }

    "support density queries against join attributes" in {
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      val envelope = new ReferencedEnvelope(30, 60, 30, 60, CRS_EPSG_4326)
      query.getHints.put(DENSITY_BBOX, envelope)
      query.getHints.put(DENSITY_HEIGHT, 600)
      query.getHints.put(DENSITY_WIDTH, 400)
      query.getHints.put(DENSITY_WEIGHT, "age")
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[JoinPlan])
      val decode = DensityScan.decodeResult(envelope, 600, 400)
      val results = runQuery(query).flatMap(decode).toList
      results must containTheSameElementsAs(Seq((40.675,58.5375,21.0), (41.325,58.5375,30.0), (42.025,58.5375,0.0)))
    }

    "support density queries against full values" in {
      val query = new Query(sftName, ECQL.toFilter("name = 'bill' OR name = 'charles'"))
      val envelope = new ReferencedEnvelope(30, 60, 30, 60, CRS_EPSG_4326)
      query.getHints.put(DENSITY_BBOX, envelope)
      query.getHints.put(DENSITY_HEIGHT, 600)
      query.getHints.put(DENSITY_WIDTH, 400)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[BatchScanPlan])
      val decode = DensityScan.decodeResult(envelope, 600, 400)
      val results = runQuery(query).flatMap(decode).toList
      results must containTheSameElementsAs(Seq((40.675,58.5375,1.0), (42.025,58.5375,1.0)))
    }
  }

  "AttributeIndexEqualsStrategy" should {

    "correctly query on ints" in {
      val features = execute("age=21")
      features must haveLength(1)
      features must contain("bill")
    }

    "correctly query on longs" in {
      val features = execute("count=2")
      features must haveLength(1)
      features must contain("bill")
    }

    "correctly query on floats" in {
      val features = execute("height=12.0")
      features must haveLength(2)
      features must contain("bob", "charles")
    }

    "correctly query on floats in different precisions" in {
      val features = execute("height=10")
      features must haveLength(1)
      features must contain("alice")
    }

    "correctly query on doubles" in {
      val features = execute("weight=6.0")
      features must haveLength(2)
      features must contain("bill", "bob")
    }

    "correctly query on doubles in different precisions" in {
      val features = execute("weight=6")
      features must haveLength(2)
      features must contain("bill", "bob")
    }

    "correctly query on booleans" in {
      val features = execute("admin=false")
      features must haveLength(3)
      features must contain("bill", "bob", "charles")
    }

    "correctly query on strings" in {
      val features = execute("name='bill'")
      features must haveLength(1)
      features must contain("bill")
    }

    "correctly query on OR'd strings" in {
      val features = execute("name = 'bill' OR name = 'charles'")
      features must haveLength(2)
      features must contain("bill", "charles")
    }

    "correctly query on IN strings" in {
      val features = execute("name IN ('bill', 'charles')")
      features must haveLength(2)
      features must contain("bill", "charles")
    }

    "correctly query on OR'd strings with bboxes" in {
      val features = execute("(name = 'bill' OR name = 'charles') AND bbox(geom,40,45,50,55)")
      features must haveLength(2)
      features must contain("bill", "charles")
    }

    "correctly query on IN strings with bboxes" in {
      val features = execute("name IN ('bill', 'charles') AND bbox(geom,40,45,50,55)")
      features must haveLength(2)
      features must contain("bill", "charles")
    }

    "correctly query on redundant OR'd strings" in {
      val features = execute("(name = 'bill' OR name = 'charles') AND name = 'charles'")
      features must haveLength(1)
      features must contain("charles")
    }

    "correctly query on date objects" in {
      val features = execute("indexedDtg TEQUALS 2014-01-01T12:30:00.000Z")
      features must haveLength(1)
      features must contain("charles")
    }

    "correctly query on date strings in standard format" in {
      val features = execute("indexedDtg = '2014-01-01T12:30:00.000Z'")
      features must haveLength(1)
      features must contain("charles")
    }

    "correctly query on lists of strings" in {
      val features = execute("fingers = 'index'")
      features must haveLength(3)
      features must contain("alice", "bob", "charles")
    }

    "correctly query on lists of doubles" in {
      val features = execute("toes = 2.0")
      features must haveLength(2)
      features must contain("bill", "bob")
    }
  }

  "AttributeIndexRangeStrategy" should {

    "correctly query on ints (with nulls)" >> {
      "lt" >> {
        val features = execute("age<21")
        features must haveLength(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute("age>21")
        features must haveLength(1)
        features must contain("bob")
      }
      "lte" >> {
        val features = execute("age<=21")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute("age>=21")
        features must haveLength(2)
        features must contain("bill", "bob")
      }
      "between (inclusive)" >> {
        val features = execute("age BETWEEN 20 AND 25")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
    }

    "correctly query on longs" >> {
      "lt" >> {
        val features = execute("count<2")
        features must haveLength(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute("count>2")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "lte" >> {
        val features = execute("count<=2")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute("count>=2")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("count BETWEEN 3 AND 7")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
    }

    "correctly query on floats" >> {
      "lt" >> {
        val features = execute("height<12.0")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
      "gt" >> {
        val features = execute("height>12.0")
        features must haveLength(0)
      }
      "lte" >> {
        val features = execute("height<=12.0")
        features must haveLength(4)
        features must contain("alice", "bill", "bob", "charles")
      }
      "gte" >> {
        val features = execute("height>=12.0")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("height BETWEEN 10.0 AND 11.5")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
    }

    "correctly query on floats in different precisions" >> {
      "lt" >> {
        val features = execute("height<11")
        features must haveLength(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute("height>11")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "lte" >> {
        val features = execute("height<=11")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute("height>=11")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("height BETWEEN 11 AND 12")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
    }

    "correctly query on doubles" >> {
      "lt" >> {
        val features = execute("weight<6.0")
        features must haveLength(1)
        features must contain("alice")
      }
      "lt fraction" >> {
        val features = execute("weight<6.1")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
      "gt" >> {
        val features = execute("weight>6.0")
        features must haveLength(1)
        features must contain("charles")
      }
      "gt fractions" >> {
        val features = execute("weight>5.9")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "lte" >> {
        val features = execute("weight<=6.0")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
      "gte" >> {
        val features = execute("weight>=6.0")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("weight BETWEEN 5.5 AND 6.5")
        features must haveLength(2)
        features must contain("bill", "bob")
      }
    }

    "correctly query on doubles in different precisions" >> {
      "lt" >> {
        val features = execute("weight<6")
        features must haveLength(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute("weight>6")
        features must haveLength(1)
        features must contain("charles")
      }
      "lte" >> {
        val features = execute("weight<=6")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
      "gte" >> {
        val features = execute("weight>=6")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("weight BETWEEN 5 AND 6")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
    }

    "correctly query on strings" >> {
      "lt" >> {
        val features = execute("name<'bill'")
        features must haveLength(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute("name>'bill'")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "lte" >> {
        val features = execute("name<='bill'")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute("name>='bill'")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("name BETWEEN 'bill' AND 'bob'")
        features must haveLength(2)
        features must contain("bill", "bob")
      }
    }

    "correctly query on date objects" >> {
      "before" >> {
        val features = execute("indexedDtg BEFORE 2014-01-01T12:30:00.000Z")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
      "after" >> {
        val features = execute("indexedDtg AFTER 2013-01-01T12:30:00.000Z")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "during (exclusive)" >> {
        val features = execute("indexedDtg DURING 2012-01-01T11:00:00.000Z/2014-01-01T12:15:00.000Z")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
    }

    "correctly query on date strings in standard format" >> {
      "lt" >> {
        val features = execute("indexedDtg < '2014-01-01T12:30:00.000Z'")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
      "gt" >> {
        val features = execute("indexedDtg > '2013-01-01T12:00:00.000Z'")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("indexedDtg BETWEEN '2012-01-01T12:00:00.000Z' AND '2013-01-01T12:00:00.000Z'")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
    }

    "correctly query with attribute on right side" >> {
      "lt" >> {
        val features = execute("'bill' > name")
        features must haveLength(1)
        features must contain("alice")
      }
      "gt" >> {
        val features = execute("'bill' < name")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "lte" >> {
        val features = execute("'bill' >= name")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
      "gte" >> {
        val features = execute("'bill' <= name")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "before" >> {
        execute("2014-01-01T12:30:00.000Z AFTER indexedDtg") should throwA[CQLException]
      }
      "after" >> {
        execute("2013-01-01T12:30:00.000Z BEFORE indexedDtg") should throwA[CQLException]
      }
    }

    "correctly query on lists of strings" in {
      // note: may return duplicate results
      "lt" >> {
        val features = execute("fingers<'middle'")
        features must contain("alice", "bob", "charles")
      }
      "gt" >> {
        val features = execute("fingers>'middle'")
        features must contain("bill", "bob", "charles")
      }
      "lte" >> {
        val features = execute("fingers<='middle'")
        features must contain("alice", "bill", "bob", "charles")
      }
      "gte" >> {
        val features = execute("fingers>='middle'")
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("fingers BETWEEN 'pinkie' AND 'thumb'")
        features must contain("bill", "bob", "charles")
      }
    }

    "correctly query on lists of doubles" in {
      // note: may return duplicate results
      "lt" >> {
        val features = execute("toes<2.0")
        features must contain("alice", "bill")
      }
      "gt" >> {
        val features = execute("toes>2.0")
        features must contain("bob")
      }
      "lte" >> {
        val features = execute("toes<=2.0")
        features must contain("alice", "bill", "bob")
      }
      "gte" >> {
        val features = execute("toes>=2.0")
        features must contain("bill", "bob")
      }
      "between (inclusive)" >> {
        val features = execute("toes BETWEEN 1.5 AND 2.5")
        features must contain("bill", "bob")
      }
    }

    "correctly query on not nulls" in {
      val features = execute("age IS NOT NULL")
      features must haveLength(3)
      features must contain("alice", "bill", "bob")
    }

    "correctly query on indexed attributes with nonsensical AND queries" >> {
      "redundant int query" >> {
        val features = execute("age > 25 AND age > 15")
        features must haveLength(1)
        features must contain("bob")
      }

      "int query that returns nothing" >> {
        val features = execute("age > 25 AND age < 15")
        features must haveLength(0)
      }

      "redundant float query" >> {
        val features = execute("height >= 6 AND height > 4")
        features must haveLength(4)
        features must contain("alice", "bill", "bob", "charles")
      }

      "float query that returns nothing" >> {
        val features = execute("height >= 6 AND height < 4")
        features must haveLength(0)
      }

      "redundant date query" >> {
        val features = execute("indexedDtg AFTER 2011-01-01T00:00:00.000Z AND indexedDtg AFTER 2012-02-01T00:00:00.000Z")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }

      "date query that returns nothing" >> {
        val features = execute("indexedDtg BEFORE 2011-01-01T00:00:00.000Z AND indexedDtg AFTER 2012-01-01T00:00:00.000Z")
        features must haveLength(0)
      }

      "redundant date and float query" >> {
        val features = execute("height >= 6 AND height > 4 AND indexedDtg AFTER 2011-01-01T00:00:00.000Z AND indexedDtg AFTER 2012-02-01T00:00:00.000Z")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }

      "date and float query that returns nothing" >> {
        val features = execute("height >= 6 AND height > 4 AND indexedDtg BEFORE 2011-01-01T00:00:00.000Z AND indexedDtg AFTER 2012-01-01T00:00:00.000Z")
        features must haveLength(0)
      }
    }
  }

  "AttributeIndexLikeStrategy" should {

    "correctly query on strings" in {
      val features = execute("name LIKE 'b%'")
      features must haveLength(2)
      features must contain("bill", "bob")
    }

    "correctly query on non-strings" in {
      val features = execute("age LIKE '2%'")
      features must haveLength(2)
      features must contain("alice", "bill")
    }.pendingUntilFixed("Lexicoding does not allow us to prefix search non-strings")
  }

  "AttributeIdxStrategy merging" should {
    val ff = CommonFactoryFinder.getFilterFactory2

    "merge PropertyIsEqualTo primary filters" >> {
      val q1 = ff.equals(ff.property("name"), ff.literal("1"))
      val q2 = ff.equals(ff.property("name"), ff.literal("2"))
      val qf1 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q1), None, 0L)
      val qf2 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q2), None, 0L)
      val res = FilterSplitter.tryMergeAttrStrategy(qf1, qf2)
      res must not(beNull)
      res.primary must beSome(ff.or(q1, q2))
    }

    "merge PropertyIsEqualTo on multiple ORs" >> {
      val q1 = ff.equals(ff.property("name"), ff.literal("1"))
      val q2 = ff.equals(ff.property("name"), ff.literal("2"))
      val q3 = ff.equals(ff.property("name"), ff.literal("3"))
      val qf1 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q1), None, 0L)
      val qf2 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q2), None, 0L)
      val qf3 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q3), None, 0L)
      val res = FilterSplitter.tryMergeAttrStrategy(FilterSplitter.tryMergeAttrStrategy(qf1, qf2), qf3)
      res must not(beNull)
      res.primary.map(decomposeOr) must beSome(containTheSameElementsAs(Seq[Filter](q1, q2, q3)))
    }

    "merge PropertyIsEqualTo when secondary matches" >> {
      val bbox = ff.bbox("geom", 1, 2, 3, 4, "EPSG:4326")
      val q1 = ff.equals(ff.property("name"), ff.literal("1"))
      val q2 = ff.equals(ff.property("name"), ff.literal("2"))
      val q3 = ff.equals(ff.property("name"), ff.literal("3"))
      val qf1 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q1), Some(bbox), 0L)
      val qf2 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q2), Some(bbox), 0L)
      val qf3 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q3), Some(bbox), 0L)
      val res = FilterSplitter.tryMergeAttrStrategy(FilterSplitter.tryMergeAttrStrategy(qf1, qf2), qf3)
      res must not(beNull)
      res.primary.map(decomposeOr) must beSome(containTheSameElementsAs(Seq[Filter](q1, q2, q3)))
      res.secondary must beSome(bbox)
    }

    "not merge PropertyIsEqualTo when secondary does not match" >> {
      val bbox = ff.bbox("geom", 1, 2, 3, 4, "EPSG:4326")
      val q1 = ff.equals(ff.property("name"), ff.literal("1"))
      val q2 = ff.equals(ff.property("name"), ff.literal("2"))
      val qf1 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q1), Some(bbox), 0L)
      val qf2 = FilterStrategy(new AttributeIndex(ds, sft, "name", Seq.empty, IndexMode.ReadWrite), Some(q2), None, 0L)
      val res = FilterSplitter.tryMergeAttrStrategy(qf1, qf2)
      res must beNull
    }
  }

  "AttributeIndexIterator" should {
    "be run when requesting extra index-encoded attributes" in {
      val sftName = "AttributeIndexIteratorTriggerTest"
      val spec = "name:String:index=join,age:Integer:index-value=true,dtg:Date:index=join,*geom:Point:srid=4326;" +
          "override.index.dtg.join=true"
      val sft = SimpleFeatureTypes.createType(sftName, spec)
      ds.createSchema(sft)
      val qps = ds.getQueryPlan(new Query(sftName, ECQL.toFilter("name='bob'"), Array("geom", "dtg", "name", "age")))
      forall(qps)(qp => qp.filter.index.name mustEqual JoinIndex.name)
    }
  }
}
