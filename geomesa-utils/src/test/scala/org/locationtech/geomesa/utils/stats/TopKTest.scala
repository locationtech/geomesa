/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Long => jLong}
import java.util.Date

import org.locationtech.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.{GeoToolsDateFormat, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TopKTest extends Specification {

  val sft = SimpleFeatureTypes.createType("topk", "name:String,score:Long,height:Double,dtg:Date,*geom:Point:srid=4326")

  val builder = new SimpleFeatureBuilder(sft)

  val features1 = (0 until 100).map { i =>
    if (i < 10) {
      builder.addAll(Array[AnyRef]("name10", "10", "10.0", "2010-01-01T00:00:00.000Z", "POINT(10 0)"))
    } else if (i < 15) {
      builder.addAll(Array[AnyRef]("name15", "15", "15.0", "2015-01-01T00:00:00.000Z", "POINT(15 0)"))
    } else if (i < 30) {
      builder.addAll(Array[AnyRef]("name30", "30", "30.0", "2030-01-01T00:00:00.000Z", "POINT(30 0)"))
    } else if (i < 50) {
      builder.addAll(Array[AnyRef]("name50", "50", "50.0", "2050-01-01T00:00:00.000Z", "POINT(50 0)"))
    } else {
      builder.addAll(Array[AnyRef]("name100", "100", "100.0", "2100-01-01T00:00:00.000Z", "POINT(100 0)"))
    }
    builder.buildFeature(i.toString)
  }

  val features2 = (0 until 100).map { i =>
    if (i < 10) {
      builder.addAll(Array[AnyRef]("name10-2", "210", "10.2", "2010-01-01T02:00:00.000Z", "POINT(10 2)"))
    } else if (i < 15) {
      builder.addAll(Array[AnyRef]("name15-2", "215", "15.2", "2015-01-01T02:00:00.000Z", "POINT(15 2)"))
    } else if (i < 30) {
      builder.addAll(Array[AnyRef]("name30-2", "230", "30.2", "2030-01-01T02:00:00.000Z", "POINT(30 2)"))
    } else if (i < 50) {
      builder.addAll(Array[AnyRef]("name50-2", "250", "50.2", "2050-01-01T02:00:00.000Z", "POINT(50 2)"))
    } else {
      builder.addAll(Array[AnyRef]("name100-2", "2100", "100.2", "2100-01-01T02:00:00.000Z", "POINT(100 2)"))
    }
    builder.buildFeature(i.toString)
  }

  def createStat[T](attribute: String): TopK[T] = Stat(sft, s"TopK($attribute)").asInstanceOf[TopK[T]]

  def stringStat = createStat[String]("name")
  def longStat   = createStat[jLong]("score")
  def doubleStat = createStat[jDouble]("height")
  def dateStat   = createStat[Date]("dtg")
  def geomStat   = createStat[Geometry]("geom")

  "TopK stat" should {

    "work with strings" >> {
      "be empty initially" >> {
        val stat = stringStat
        stat.isEmpty must beTrue
        stat.topK(10) must beEmpty
      }

      "correctly calculate values"  >> {
        val stat = stringStat
        features1.foreach(stat.observe)
        stat.isEmpty must beFalse
        stat.size mustEqual 5
        stat.topK(10).toSeq mustEqual Seq(("name100", 50), ("name50", 20), ("name30", 15), ("name10", 10), ("name15", 5))
      }

      "serialize and deserialize" >> {
        val stat = stringStat
        features1.foreach(stat.observe)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[TopK[String]]
        unpacked.asInstanceOf[TopK[String]].size mustEqual stat.size
        unpacked.asInstanceOf[TopK[String]].property mustEqual stat.property
        unpacked.asInstanceOf[TopK[String]].toJson mustEqual stat.toJson
        unpacked.isEquivalent(stat) must beTrue
      }

      "serialize and deserialize empty stats" >> {
        val stat = stringStat
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[TopK[String]]
        unpacked.asInstanceOf[TopK[String]].size mustEqual stat.size
        unpacked.asInstanceOf[TopK[String]].property mustEqual stat.property
        unpacked.asInstanceOf[TopK[String]].toJson mustEqual stat.toJson
        unpacked.isEquivalent(stat) must beTrue
      }

      "deserialize as immutable value" >> {
        val stat = stringStat
        features1.foreach(stat.observe)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)

        unpacked must beAnInstanceOf[TopK[String]]
        unpacked.asInstanceOf[TopK[String]].size mustEqual stat.size
        unpacked.asInstanceOf[TopK[String]].property mustEqual stat.property
        unpacked.asInstanceOf[TopK[String]].toJson mustEqual stat.toJson
        unpacked.isEquivalent(stat) must beTrue

        unpacked.clear must throwAn[Exception]
        unpacked.+=(stat) must throwAn[Exception]
        unpacked.observe(features1.head) must throwAn[Exception]
        unpacked.unobserve(features1.head) must throwAn[Exception]
      }

      "combine two TopKs" >> {
        val stat = stringStat
        val stat2 = stringStat

        features1.foreach(stat.observe)
        features2.foreach(stat2.observe)

        stat2.size mustEqual 5
        stat2.topK(10).toSeq mustEqual Seq(("name100-2", 50), ("name50-2", 20), ("name30-2", 15), ("name10-2", 10), ("name15-2", 5))

        stat += stat2

        stat.size mustEqual 10

        stat.topK(10).toSeq mustEqual Seq(("name100", 50), ("name100-2", 50), ("name50", 20), ("name50-2", 20),
          ("name30", 15), ("name30-2", 15), ("name10", 10), ("name10-2", 10), ("name15", 5), ("name15-2", 5))

        stat2.size mustEqual 5
        stat2.topK(10).toSeq mustEqual Seq(("name100-2", 50), ("name50-2", 20), ("name30-2", 15), ("name10-2", 10), ("name15-2", 5))
      }

      "clear" >> {
        val stat = stringStat
        features1.foreach(stat.observe)
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        stat.topK(10).toSeq must beEmpty
      }
    }

    "work with longs" >> {
      "correctly calculate values"  >> {
        val stat = longStat
        features1.foreach(stat.observe)
        stat.isEmpty must beFalse
        stat.size mustEqual 5
        stat.topK(10).toSeq mustEqual Seq((100L, 50), (50L, 20), (30L, 15), (10L, 10), (15L, 5))
      }

      "serialize and deserialize" >> {
        val stat = longStat
        features1.foreach(stat.observe)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[TopK[String]]
        unpacked.asInstanceOf[TopK[String]].size mustEqual stat.size
        unpacked.asInstanceOf[TopK[String]].property mustEqual stat.property
        unpacked.asInstanceOf[TopK[String]].toJson mustEqual stat.toJson
        unpacked.isEquivalent(stat) must beTrue
      }
    }

    "work with doubles" >> {
      "correctly calculate values"  >> {
        val stat = doubleStat
        features1.foreach(stat.observe)
        stat.isEmpty must beFalse
        stat.size mustEqual 5
        stat.topK(10).toSeq mustEqual Seq((100.0, 50), (50.0, 20), (30.0, 15), (10.0, 10), (15.0, 5))
      }

      "serialize and deserialize" >> {
        val stat = doubleStat
        features1.foreach(stat.observe)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[TopK[String]]
        unpacked.asInstanceOf[TopK[String]].size mustEqual stat.size
        unpacked.asInstanceOf[TopK[String]].property mustEqual stat.property
        unpacked.asInstanceOf[TopK[String]].toJson mustEqual stat.toJson
        unpacked.isEquivalent(stat) must beTrue
      }
    }

    "work with dates" >> {

      def toDate(year: Int) = java.util.Date.from(java.time.LocalDateTime.parse(f"2$year%03d-01-01T00:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))

      "correctly calculate values"  >> {
        val stat = dateStat
        features1.foreach(stat.observe)
        stat.isEmpty must beFalse
        stat.size mustEqual 5
        stat.topK(10).toSeq mustEqual Seq((toDate(100), 50), (toDate(50), 20), (toDate(30), 15), (toDate(10), 10), (toDate(15), 5))
      }

      "serialize and deserialize" >> {
        val stat = dateStat
        features1.foreach(stat.observe)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[TopK[String]]
        unpacked.asInstanceOf[TopK[String]].size mustEqual stat.size
        unpacked.asInstanceOf[TopK[String]].property mustEqual stat.property
        unpacked.asInstanceOf[TopK[String]].toJson mustEqual stat.toJson
        unpacked.isEquivalent(stat) must beTrue
      }
    }

    "work with geometries" >> {

      def toGeom(lon: Int) = WKTUtils.read(s"POINT($lon 0)")

      "correctly calculate values"  >> {
        val stat = geomStat
        features1.foreach(stat.observe)
        stat.isEmpty must beFalse
        stat.size mustEqual 5
        stat.topK(10).toSeq mustEqual Seq((toGeom(100), 50), (toGeom(50), 20), (toGeom(30), 15), (toGeom(10), 10), (toGeom(15), 5))
      }

      "serialize and deserialize" >> {
        val stat = geomStat
        features1.foreach(stat.observe)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[TopK[String]]
        unpacked.asInstanceOf[TopK[String]].size mustEqual stat.size
        unpacked.asInstanceOf[TopK[String]].property mustEqual stat.property
        unpacked.asInstanceOf[TopK[String]].toJson mustEqual stat.toJson
        unpacked.isEquivalent(stat) must beTrue
      }
    }
  }
}
