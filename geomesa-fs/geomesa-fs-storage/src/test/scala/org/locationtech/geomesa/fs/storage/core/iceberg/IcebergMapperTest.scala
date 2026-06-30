/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.types.Types
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemContext
import org.locationtech.geomesa.fs.storage.core.schemes.{DateTimeScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.DateParsing
import org.locationtech.jts.geom.Point
import org.specs2.mutable.SpecificationWithJUnit

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Date

class IcebergMapperTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  val context = FileSystemContext(URI.create("file:///tmp/"), Map.empty, None)
  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val schema = SimpleFeatureIcebergSchema(sft, context.conf).schema
  val sf = ScalaSimpleFeature.create(sft, "", "goodbye", "11", "2026-05-06T11:12:13", "POINT (10 10)")
  val dtg = sf.getAttribute("dtg").asInstanceOf[Date].getTime * 1000 // in microseconds

  // note: we compare expressions as strings as column refs don't implement 'equals'

  "IcebergMapper" should {
    "map string attributes" in {
      val scheme = PartitionSchemeFactory.load(sft, "attribute:attribute=name")
      val partition = scheme.getPartition(sf)
      partition.value mustEqual "goodbye"
      val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
      fields must haveLength(1)
      fields.head.name() mustEqual "name"
      fields.head.transform().isIdentity must beTrue
      fields.head.transform().asInstanceOf[Transform[String, String]].bind(Types.StringType.get()).apply("goodbye") mustEqual "goodbye"
      scheme.getCoveringExpression(partition).toString mustEqual Expressions.equal("name", "goodbye").toString
    }
    "map string attributes with width" in {
      val scheme = PartitionSchemeFactory.load(sft, "attribute:attribute=name:width=4")
      val partition = scheme.getPartition(sf)
      partition.value mustEqual "good"
      val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
      fields must haveLength(1)
      fields.head.name() must startWith("name")
      fields.head.transform().toString mustEqual "truncate[4]"
      fields.head.transform().asInstanceOf[Transform[String, String]].bind(Types.StringType.get()).apply("goodbye") mustEqual "good"
      scheme.getCoveringExpression(partition).toString mustEqual
        Expressions.equal(Expressions.truncate[String]("name", 4), "good").toString
    }
    "map int attributes" in {
      val scheme = PartitionSchemeFactory.load(sft, "attribute:attribute=age")
      val partition = scheme.getPartition(sf)
      partition.value mustEqual "11"
      val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
      fields must haveLength(1)
      fields.head.name() mustEqual "age"
      fields.head.transform().isIdentity must beTrue
      fields.head.transform().asInstanceOf[Transform[Int, Int]].bind(Types.IntegerType.get()).apply(11) mustEqual 11
      scheme.getCoveringExpression(partition).toString mustEqual Expressions.equal("age", 11).toString
    }
    "map int attributes with divisor" in {
      val scheme = PartitionSchemeFactory.load(sft, "attribute:attribute=age:divisor=10")
      val partition = scheme.getPartition(sf)
      partition.value mustEqual "10"
      val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
      fields must haveLength(1)
      fields.head.name() must startWith("age")
      fields.head.transform().toString mustEqual "truncate[10]"
      fields.head.transform().asInstanceOf[Transform[Int, Int]].bind(Types.IntegerType.get()).apply(11) mustEqual 10
      scheme.getCoveringExpression(partition).toString mustEqual
        Expressions.equal(Expressions.truncate[Integer]("age", 10), Int.box(10)).toString
    }
    "map hour scheme" in {
      val scheme = PartitionSchemeFactory.load(sft, "hours")
      val partition = scheme.getPartition(sf)
      val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
      fields must haveLength(1)
      fields.head.name() mustEqual "dtg_hour"
      fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual
        partition.value.toInt
      scheme.getCoveringExpression(partition).toString mustEqual
        Expressions.equal(Expressions.hour[Integer]("dtg"), Integer.valueOf(partition.value)).toString
    }
    "map day scheme" in {
      val scheme = PartitionSchemeFactory.load(sft, "days")
      val partition = scheme.getPartition(sf)
      // day scheme is handled differently than others, where the value is a formatted date string instead of an int
      val expected =
        ChronoUnit.DAYS.between(DateTimeScheme.Epoch, DateParsing.parse(partition.value, DateTimeFormatter.ISO_LOCAL_DATE)).toInt
      val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
      fields must haveLength(1)
      fields.head.name() mustEqual "dtg_day"
      fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
      scheme.getCoveringExpression(partition).toString mustEqual
        Expressions.equal(Expressions.day[Integer]("dtg"), Int.box(expected)).toString
    }
    "map month scheme" in {
      val scheme = PartitionSchemeFactory.load(sft, "months")
      val partition = scheme.getPartition(sf)
      val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
      fields must haveLength(1)
      fields.head.name() mustEqual "dtg_month"
      fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual
        partition.value.toInt
      scheme.getCoveringExpression(partition).toString mustEqual
        Expressions.equal(Expressions.month[Integer]("dtg"), Integer.valueOf(partition.value)).toString
    }
    "map year scheme" in {
      val scheme = PartitionSchemeFactory.load(sft, "years")
      val partition = scheme.getPartition(sf)
      val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
      fields must haveLength(1)
      fields.head.name() mustEqual "dtg_year"
      fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual
        partition.value.toInt
      scheme.getCoveringExpression(partition).toString mustEqual
        Expressions.equal(Expressions.year[Integer]("dtg"), Integer.valueOf(partition.value)).toString
    }
    "map z2 4/8-bit scheme" in {
      val fullZValue = Z2SFC.hexEncode(sf.getDefaultGeometry.asInstanceOf[Point].getX, sf.getDefaultGeometry.asInstanceOf[Point].getY)
      foreach(Seq(4, 8)) { bits =>
        val scheme = PartitionSchemeFactory.load(sft, s"z2:bits=$bits")
        val partition = scheme.getPartition(sf)
        val fields = scheme.spec(PartitionSpec.builderFor(schema)).build().fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "__geom_z2___trunc"
        scheme.getCoveringExpression(partition).toString mustEqual
          Expressions.equal(Expressions.truncate[String]("__geom_z2__", bits / 4) , fullZValue.take(bits / 4)).toString
      }
    }
    "not map unsupported schemas" in {
      foreach(Seq("hours:step=2", "weekly")) { unsupported =>
        PartitionSchemeFactory.load(sft, unsupported) must throwAn[IllegalArgumentException]
      }
    }
  }
}
