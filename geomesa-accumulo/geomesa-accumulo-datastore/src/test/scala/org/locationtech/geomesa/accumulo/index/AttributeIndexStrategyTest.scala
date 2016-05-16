/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.cql2.CQLException
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.tables.AttributeTable
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AttributeIndexStrategyTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "name:String:index=full,age:Integer:index=true,count:Long:index=true," +
      "weight:Double:index=true,height:Float:index=true,admin:Boolean:index=true," +
      "geom:Point:srid=4326,dtg:Date,indexedDtg:Date:index=true,fingers:List[String]:index=true," +
      "toes:List[Double]:index=true,track:String;geomesa.indexes.enabled='attr_idx,records'"

  val geom = WKTUtils.read("POINT(45.0 49.0)")

  val df = ISODateTimeFormat.dateTime()

  val aliceDate   = df.parseDateTime("2012-01-01T12:00:00.000Z").toDate
  val billDate    = df.parseDateTime("2013-01-01T12:00:00.000Z").toDate
  val bobDate     = df.parseDateTime("2014-01-01T12:00:00.000Z").toDate
  val charlesDate = df.parseDateTime("2014-01-01T12:30:00.000Z").toDate

  val aliceFingers   = List("index")
  val billFingers    = List("ring", "middle")
  val bobFingers     = List("index", "thumb", "pinkie")
  val charlesFingers = List("thumb", "ring", "index", "pinkie", "middle")

  val features = Seq(
    Array("alice",   20,   1, 5.0, 10.0F, true,  geom, aliceDate, aliceDate, aliceFingers, List(1.0), "track1"),
    Array("bill",    21,   2, 6.0, 11.0F, false, geom, billDate, billDate, billFingers, List(1.0, 2.0), "track2"),
    Array("bob",     30,   3, 6.0, 12.0F, false, geom, bobDate, bobDate, bobFingers, List(3.0, 2.0, 5.0), "track1"),
    Array("charles", null, 4, 7.0, 12.0F, false, geom, charlesDate, charlesDate, charlesFingers, List(), "track1")
  ).map { entry =>
    val feature = new ScalaSimpleFeature(entry.head.toString, sft)
    feature.setAttributes(entry.asInstanceOf[Array[AnyRef]])
    feature
  }

  addFeatures(features)

  def execute(filter: String): List[String] = {
    val query = new Query(sftName, ECQL.toFilter(filter))
    forall(ds.getQueryPlan(query))(_.table must endWith(AttributeTable.suffix))
    val results = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features())
    results.map(_.getAttribute("name").toString).toList
  }

  def runQuery(query: Query): Iterator[SimpleFeature] = {
    forall(ds.getQueryPlan(query))(_.table must endWith(AttributeTable.suffix))
    SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features())
  }

  "AttributeIndexStrategy" should {
    "print values" in {
      skipped("used for debugging")
      val scanner = connector.createScanner(ds.getTableName(sftName, AttributeTable), new Authorizations())
      val prefix = AttributeTable.getRowPrefix(sft, sft.indexOf("fingers"))
      scanner.setRange(AccRange.prefix(new Text(prefix)))
      scanner.asScala.foreach(println)
      println()
      success
    }

    "all attribute filters should be applied to SFFI" in {
      val filter = andFilters(Seq(ECQL.toFilter("name LIKE 'b%'"), ECQL.toFilter("count<27"), ECQL.toFilter("age<29")))
      val results = execute(ECQL.toCQL(filter))
      results must haveLength(1)
      results must contain ("bill")
    }

    "support bin queries with join queries" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      query.getHints.put(BIN_TRACK_KEY, "name")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      explain(query).split("\n").map(_.trim).filter(_.startsWith("Join Plan:")) must haveLength(1)
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq("bill", "bob", "charles").map(_.hashCode.toString))
    }

    "support bin queries against index values" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      query.getHints.put(BIN_TRACK_KEY, "dtg")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      explain(query).split("\n").filter(_.startsWith("Join Table:")) must beEmpty
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq(billDate, bobDate, charlesDate).map(_.hashCode.toString))
    }

    "support bin queries against full values" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("name>'amy'"))
      query.getHints.put(BIN_TRACK_KEY, "count")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      explain(query).split("\n").filter(_.startsWith("Join Table:")) must beEmpty
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq(2, 3, 4).map(_.hashCode.toString))
    }

    "correctly query equals with date ranges" in {
      val features = execute("height = 12.0 AND " +
          "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(1)
      features must contain("bob")
    }

    "correctly query lt with date ranges" in {
      val features = execute("height < 12.0 AND " +
          "dtg DURING 2011-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z")
      features must haveLength(1)
      features must contain("alice")
    }

    "correctly query lte with date ranges" in {
      val features = execute("height <= 12.0 AND " +
          "dtg DURING 2013-01-01T00:00:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(2)
      features must contain("bill", "bob")
    }

    "correctly query gt with date ranges" in {
      val features = execute("height > 11.0 AND " +
          "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(1)
      features must contain("bob")
    }

    "correctly query gte with date ranges" in {
      val features = execute("height >= 11.0 AND " +
          "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(1)
      features must contain("bob")
    }

    "correctly query between with date ranges" in {
      val features = execute("height between 11.0 AND 12.0 AND " +
          "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(1)
      features must contain("bob")
    }

    "support sampling" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
    }

    "support sampling with cql" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a' AND track > 'track'"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
    }

    "support sampling with transformations" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"), Array("name", "geom"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling with cql and transformations" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a' AND track > 'track'"), Array("name", "geom"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.2f))
      val results = runQuery(query).toList
      results must haveLength(1)
      results.head.getAttributeCount mustEqual 2
    }

    "support sampling by thread" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      query.getHints.put(SAMPLE_BY_KEY, "track")
      val results = runQuery(query).toList
      results must haveLength(2)
      results.map(_.getAttribute("track")) must containTheSameElementsAs(Seq("track1", "track2"))
    }

    "support sampling with bin queries" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      // important - id filters will create multiple ranges and cause multiple iterators to be created
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(BIN_TRACK_KEY, "name")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      // have to evaluate attributes before pulling into collection, as the same sf is reused
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(2)
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
      "lt" >> {
        val features = execute("fingers<'middle'")
        features must haveLength(3)
        features must contain("alice", "bob", "charles")
      }
      "gt" >> {
        val features = execute("fingers>'middle'")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "lte" >> {
        val features = execute("fingers<='middle'")
        features must haveLength(4)
        features must contain("alice", "bill", "bob", "charles")
      }
      "gte" >> {
        val features = execute("fingers>='middle'")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("fingers BETWEEN 'pinkie' AND 'thumb'")
        features must haveLength(3)
        features must contain("bill", "bob", "charles")
      }
    }

    "correctly query on lists of doubles" in {
      "lt" >> {
        val features = execute("toes<2.0")
        features must haveLength(2)
        features must contain("alice", "bill")
      }
      "gt" >> {
        val features = execute("toes>2.0")
        features must haveLength(1)
        features must contain("bob")
      }
      "lte" >> {
        val features = execute("toes<=2.0")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
      "gte" >> {
        val features = execute("toes>=2.0")
        features must haveLength(2)
        features must contain("bill", "bob")
      }
      "between (inclusive)" >> {
        val features = execute("toes BETWEEN 1.5 AND 2.5")
        features must haveLength(2)
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
  }

  "AttributeIdxStrategy merging" should {
    val ff = CommonFactoryFinder.getFilterFactory2

    "merge PropertyIsEqualTo primary filters" >> {
      val q1 = ff.equals(ff.property("prop"), ff.literal("1"))
      val q2 = ff.equals(ff.property("prop"), ff.literal("2"))
      val qf1 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q1), None)
      val qf2 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q2), None)
      val res = AttributeIdxStrategy.tryMergeAttrStrategy(qf1, qf2)
      "result must not be null" >> { res must not beNull }
      "result must have two primary filters" >> { res.primary.length must equalTo(2) }
      "result filters must be on 'prop'" >> { res.primary.flatMap { f => DataUtilities.attributeNames(f) } must contain(exactly("prop", "prop")) }
    }

    "merge PropertyIsEqualTo on multiple ORs" >> {
      import AttributeIdxStrategy._

      val q1 = ff.equals(ff.property("prop"), ff.literal("1"))
      val q2 = ff.equals(ff.property("prop"), ff.literal("2"))
      val q3 = ff.equals(ff.property("prop"), ff.literal("3"))
      val qf1 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q1), None)
      val qf2 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q2), None)
      val qf3 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q3), None)
      val res = tryMergeAttrStrategy(tryMergeAttrStrategy(qf1, qf2), qf3)
      "result must not be null" >> { res must not beNull }
      "result must have three primary filters" >> { res.primary.length must equalTo(3) }
      "result filters must be on 'prop'" >> { res.primary.flatMap { f => DataUtilities.attributeNames(f) } must contain(exactly("prop", "prop", "prop")) }
    }

    "merge PropertyIsEqualTo when secondary matches" >> {
      import AttributeIdxStrategy._
      val bbox = ff.bbox("geom", 1, 2, 3, 4, "EPSG:4326")
      val q1 = ff.equals(ff.property("prop"), ff.literal("1"))
      val q2 = ff.equals(ff.property("prop"), ff.literal("2"))
      val q3 = ff.equals(ff.property("prop"), ff.literal("3"))
      val qf1 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q1), Some(bbox))
      val qf2 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q2), Some(bbox))
      val qf3 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q3), Some(bbox))
      val res = tryMergeAttrStrategy(tryMergeAttrStrategy(qf1, qf2), qf3)
      "result must not be null" >> { res must not beNull }
      "result must have three primary filters" >> { res.primary.length must equalTo(3) }
      "result filters must be on 'prop'" >> { res.primary.flatMap { f => DataUtilities.attributeNames(f) } must contain(exactly("prop", "prop", "prop")) }
      "result secondary must be bbox" >> { res.secondary.exists(_.equals(bbox)) }
    }

    "not merge PropertyIsEqualTo when secondary does not match" >> {
      import AttributeIdxStrategy._
      val bbox = ff.bbox("geom", 1, 2, 3, 4, "EPSG:4326")
      val q1 = ff.equals(ff.property("prop"), ff.literal("1"))
      val q2 = ff.equals(ff.property("prop"), ff.literal("2"))
      val qf1 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q1), Some(bbox))
      val qf2 = new QueryFilter(StrategyType.ATTRIBUTE, Seq(q2), None)
      val res = tryMergeAttrStrategy(qf1, qf2)
      "result must be null" >> { res must beNull }
    }

  }
}
