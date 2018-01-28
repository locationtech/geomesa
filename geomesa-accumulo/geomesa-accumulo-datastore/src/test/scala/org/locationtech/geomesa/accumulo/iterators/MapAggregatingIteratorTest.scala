/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.accumulo.iterators

import java.io.{Serializable => JSerializable}
import java.util.{Map => JMap}

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.io.Source
import scala.languageFeature.postfixOps
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class MapAggregatingIteratorTest extends Specification with TestWithDataStore {

  sequential

  override def spec = "an_id:Integer,map:Map[String,Integer],dtg:Date,geom:Geometry:srid=4326;geomesa.mixed.geometries=true"

  val testData : Map[String,String] = {
    val source = Source.fromInputStream(getClass.getResourceAsStream("/test-lines.tsv"))
    val map = source.getLines().toList.map(_.split("\t")).map(l => l(0) -> l(1)).toMap
    source.close()
    map
  }

  def getQuery(query: String): Query = {
    val q = new Query(sftName, ECQL.toFilter(query))
    q.getHints.put(QueryHints.MAP_AGGREGATION, "map")
    q
  }

  val randomSeed = 62

  "MapAggregatingIterator" should {

    "calculate correct aggregated totals" in {
      val random = new Random(randomSeed)

      val feats = (0 until 150).map { i =>
        val id = i.toString
        val map = Map("a" -> i, "b" -> i * 2, (if (random.nextBoolean()) "c" else "d") -> random.nextInt(10)).asJava
        val dtg = "2012-01-01T19:00:00Z"
        val geom = "POINT(-77 38)"
        ScalaSimpleFeatureFactory.buildFeature(sft, Array(id, map, dtg, geom), id)
      }

      clearFeatures()
      addFeatures(feats)

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = SelfClosingIterator(fs.getFeatures(q).features).toList
      results must haveLength(1)

      val aggregated = results.map(_.getAttribute("map").asInstanceOf[JMap[String, Int]].asScala).head

      aggregated("a") should be equalTo (0 until 150).sum

      aggregated("b") should be equalTo (0 until 150).map(_ * 2).sum

      val c = aggregated("c")
      c must beLessThanOrEqualTo(150 * 10)
      c must beGreaterThanOrEqualTo(0)

      val d = aggregated("d")
      d must beLessThanOrEqualTo(150 * 10)
      d must beGreaterThanOrEqualTo(0)
    }

    "correctly filter dates" in {
      val random = new Random(randomSeed)

      val feats = (0 until 200).map { i =>
        val id = i.toString
        val map = Map("a" -> i, "b" -> i * 2, (if (random.nextBoolean()) "c" else "d") -> random.nextInt(10)).asJava
        val dtg = if (i < 100) "2012-01-01T19:00:00Z" else "2012-01-01T23:30:00Z"
        val geom = "POINT(-77 38)"
        ScalaSimpleFeatureFactory.buildFeature(sft, Array(id, map, dtg, geom), id)
      }

      clearFeatures()
      addFeatures(feats)

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = SelfClosingIterator(fs.getFeatures(q).features).toList
      results must haveLength(1)

      val aggregated = results.map(_.getAttribute("map").asInstanceOf[JMap[String, Int]].asScala).head

      aggregated("a") should be equalTo (0 until 100).sum

      aggregated("b") should be equalTo (0 until 100).map(_ * 2).sum

      val c = aggregated("c")
      c must beLessThanOrEqualTo(100 * 10)
      c must beGreaterThanOrEqualTo(0)

      val d = aggregated("d")
      d must beLessThanOrEqualTo(100 * 10)
      d must beGreaterThanOrEqualTo(0)
    }

    "do aggregation on a single feature" in {
      val id = "0"
      val map = Map("a" -> 0, "b" -> 0 * 2, "c" -> 9).asJava
      val dtg = "2012-01-01T19:00:00Z"
      val geom = testData("[POLYGON] Charlottesville")
      val feature = ScalaSimpleFeatureFactory.buildFeature(sft, Array(id, map, dtg, geom), id)

      clearFeatures()
      addFeatures(Seq(feature))

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and " +
          "BBOX(geom, -78.598118, 37.992204, -78.337364, 38.091238)")

      val results = SelfClosingIterator(fs.getFeatures(q).features).toList
      results must haveLength(1)

      val aggregated = results.map(_.getAttribute("map").asInstanceOf[JMap[String, Int]].asScala).head

      aggregated("a") should be equalTo 0

      aggregated("b") should be equalTo 0

      aggregated("c") should be equalTo 9
    }

    "handle no features" in {

      clearFeatures()

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and " +
          "BBOX(geom, -78.598118, 37.992204, -78.337364, 38.091238)")

      val results = fs.getFeatures(q)

      val features = SelfClosingIterator(results.features).toList
      features must beEmpty
    }

    "do aggregation on a realistic set" in {

      val feats = Seq[Array[AnyRef]](
        Array("1", Map("a" -> 1, "b" -> 2, "c" -> 9).asJava, "2012-01-01T19:01:00Z", testData("[POLYGON] Charlottesville")),
        Array("2", Map("a" -> 2, "b" -> 2, "d" -> 9).asJava, "2012-01-01T19:02:00Z", testData("[MULTIPOLYGON] test box")),
        Array("3", Map("a" -> 3, "b" -> 2, "c" -> 3).asJava, "2012-01-01T19:03:00Z", testData("[LINE] test line")),
        Array("4", Map("a" -> 4, "b" -> 2, "d" -> 3).asJava, "2012-01-01T19:04:00Z", testData("[LINE] Cherry Avenue segment")),
        Array("5", Map("a" -> 5, "b" -> 2, "c" -> 6).asJava, "2012-01-01T19:05:00Z", testData("[MULTILINE] Cherry Avenue entirety"))
      ).map(a => ScalaSimpleFeatureFactory.buildFeature(sft, a, a(0).asInstanceOf[String]))

      clearFeatures()
      addFeatures(feats)

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and " +
          "BBOX(geom, -78.598118, 37.992204, -78.337364, 38.091238)")

      val results = SelfClosingIterator(fs.getFeatures(q).features).toList
      results must haveLength(1)

      val aggregated = results.map(_.getAttribute("map").asInstanceOf[JMap[String, Int]].asScala).head

      aggregated("a") mustEqual 10
      aggregated("b") mustEqual 6
      aggregated("c") mustEqual 15
      aggregated("d") mustEqual 3
    }
  }
}

@RunWith(classOf[JUnitRunner])
class MapAggregatingIteratorDoubleTest extends Specification with TestWithDataStore {

  override val spec = "an_id:Integer,map:Map[Double,Integer],dtg:Date,geom:Geometry:srid=4326;geomesa.mixed.geometries=true"

  val randomSeed = 62
  val random = new Random(randomSeed)
  val key1 = 5.0
  val key2 = Double.MinValue
  val key3 = Double.MaxValue
  val key4 = 0.0

  val features = (0 until 150).map { i =>
    val id = i.toString
    val map = Map(key1 -> i, key2 -> i * 2, (if(random.nextBoolean()) key3 else key4) -> random.nextInt(10)).asJava
    val dtg = "2012-01-01T19:00:00Z"
    val geom = "POINT(-77 38)"
    ScalaSimpleFeatureFactory.buildFeature(sft, Array(id, map, dtg, geom), id)
  }

  addFeatures(features)

  def getQuery(query: String): Query = {
    val q = new Query(sftName, ECQL.toFilter(query))
    q.getHints.put(QueryHints.MAP_AGGREGATION, "map")
    q
  }

  "MapAggregatingIterator with Double key" should {
    "calculate correct aggregated totals" in {

      val q = getQuery("(dtg between '2012-01-01T18:00:00.000Z' AND '2012-01-01T23:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = SelfClosingIterator(fs.getFeatures(q).features).toList
      results must haveLength(1)

      val aggregated = results.map(_.getAttribute("map").asInstanceOf[JMap[Double, Int]].asScala).head

      aggregated(key1) should be equalTo (0 until 150).sum

      aggregated(key2) should be equalTo (0 until 150).map(_ * 2).sum

      val c = aggregated(key3)
      c must beLessThanOrEqualTo(150 * 10)
      c must beGreaterThanOrEqualTo(0)

      val d = aggregated(key4)
      d must beLessThanOrEqualTo(150 * 10)
      d must beGreaterThanOrEqualTo(0)
    }
  }
}
