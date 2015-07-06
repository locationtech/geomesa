/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQLException
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.tables.AttributeTable
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator
import org.locationtech.geomesa.accumulo.{TestWithDataStore, index}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AttributeIndexStrategyTest extends Specification with TestWithDataStore {

  override val spec = "name:String:index=true,age:Integer:index=true,count:Long:index=true," +
      "weight:Double:index=true,height:Float:index=true,admin:Boolean:index=true," +
      "geom:Geometry:srid=4326,dtg:Date:index=true," +
      "fingers:List[String]:index=true,toes:List[Double]:index=true"

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
                            dtg: Date,
                            fingers: List[String],
                            toes: List[Double])

  val geom = WKTUtils.read("POINT(45.0 49.0)")

  val features =
    Seq(TestAttributes("alice",   20,   1, 5.0, 10.0F, admin = true,  geom, dtFormat.parse("20120101 12:00:00"),
          List("index"), List(1.0)),
        TestAttributes("bill",    21,   2, 6.0, 11.0F, admin = false, geom, dtFormat.parse("20130101 12:00:00"),
          List("ring", "middle"), List(1.0, 2.0)),
        TestAttributes("bob",     30,   3, 6.0, 12.0F, admin = false, geom, dtFormat.parse("20140101 12:00:00"),
          List("index", "thumb", "pinkie"), List(3.0, 2.0, 5.0)),
        TestAttributes("charles", null, 4, 7.0, 12.0F, admin = false, geom, dtFormat.parse("20140101 12:30:00"),
          List("thumb", "ring", "index", "pinkie", "middle"), List()))
    .map { entry =>
      val feature = builder.buildFeature(entry.name)
      feature.setDefaultGeometry(entry.geom)
      feature.setAttributes(entry.productIterator.toArray.asInstanceOf[Array[AnyRef]])
      feature
    }

  addFeatures(features)

  val queryPlanner = new QueryPlanner(sft, ds.getFeatureEncoding(sft), ds.getIndexSchemaFmt(sftName), ds,
    ds.strategyHints(sft), ds.getGeomesaVersion(sft))

  def execute(filter: String): List[String] = {
    val query = new Query(sftName, ECQL.toFilter(filter))
    val results = queryPlanner.runQuery(query, Some(StrategyType.ATTRIBUTE))
    results.map(_.getAttribute("name").toString).toList
  }

  "AttributeIndexStrategy" should {
    "print values" in {
      skipped("used for debugging")
      val scanner = connector.createScanner(ds.getAttributeTable(sftName), new Authorizations())
      val prefix = AttributeTable.getAttributeIndexRowPrefix(index.getTableSharingPrefix(sft),
        sft.getDescriptor("fingers"))
      scanner.setRange(AccRange.prefix(prefix))
      scanner.asScala.foreach(println)
      println()
      success
    }

    "all attribute filters should be applied to SFFI" in {
      val filter = andFilters(Seq(ECQL.toFilter("name LIKE 'b%'"), ECQL.toFilter("count<27"), ECQL.toFilter("age<29")))
      val query = new Query(sftName, filter)
      val results = queryPlanner.runQuery(query, Some(StrategyType.ATTRIBUTE))
      val resultNames = results.map(_.getAttribute("name").toString).toList
      resultNames must haveLength(1)
      resultNames must contain ("bill")
    }

    "support bin queries" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      query.getHints.put(BIN_TRACK_KEY, "name")
      val results = queryPlanner.runQuery(query, Some(StrategyType.ATTRIBUTE)).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toSeq
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq("bill", "bob", "charles").map(_.hashCode.toString))
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

    "correctly query on date objects" in {
      val features = execute("dtg TEQUALS 2014-01-01T12:30:00.000Z")
      features must haveLength(1)
      features must contain("charles")
    }

    "correctly query on date strings in standard format" in {
      val features = execute("dtg = '2014-01-01T12:30:00.000Z'")
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
        val features = execute("dtg BEFORE 2014-01-01T12:30:00.000Z")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
      "after" >> {
        val features = execute("dtg AFTER 2013-01-01T12:30:00.000Z")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "during (exclusive)" >> {
        val features = execute("dtg DURING 2012-01-01T11:00:00.000Z/2014-01-01T12:15:00.000Z")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
    }

    "correctly query on date strings in standard format" >> {
      "lt" >> {
        val features = execute("dtg < '2014-01-01T12:30:00.000Z'")
        features must haveLength(3)
        features must contain("alice", "bill", "bob")
      }
      "gt" >> {
        val features = execute("dtg > '2013-01-01T12:00:00.000Z'")
        features must haveLength(2)
        features must contain("bob", "charles")
      }
      "between (inclusive)" >> {
        val features = execute("dtg BETWEEN '2012-01-01T12:00:00.000Z' AND '2013-01-01T12:00:00.000Z'")
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
        execute("2014-01-01T12:30:00.000Z AFTER dtg") should throwA[CQLException]
      }
      "after" >> {
        execute("2013-01-01T12:30:00.000Z BEFORE dtg") should throwA[CQLException]
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
  }

  "AttributeIndexLikeStrategy" should {

    "correctly query on strings" in {
      val features = execute("name LIKE 'b%'")
      features must haveLength(2)
      features must contain("bill", "bob")
    }
  }
}
