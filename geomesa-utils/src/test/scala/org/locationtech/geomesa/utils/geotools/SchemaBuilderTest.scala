/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.{Date, UUID}

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class SchemaBuilderTest extends Specification {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  "SchemaBuilder" should {
    "build simple types" in {
      val spec = SchemaBuilder.builder().addInt("i").addLong("l").addFloat("f").addDouble("d").addString("s").spec
      spec mustEqual "i:Int,l:Long,f:Float,d:Double,s:String"
    }

    "handle date and uuid types" in {
      val spec = SchemaBuilder.builder().addDate("d").addUuid("u").spec
      spec mustEqual "d:Date,u:UUID"
    }

    "provide index when set to true" in {
      val spec = SchemaBuilder.builder()
        .addInt("i").withIndex()
        .addLong("l").withIndex()
        .addFloat("f").withIndex()
        .addDouble("d").withIndex()
        .addString("s").withIndex()
        .addDate("dt").withIndex()
        .addUuid("u").withIndex()
        .spec
      val expected = "i:Int,l:Long,f:Float,d:Double,s:String,dt:Date,u:UUID".split(",").map(_+":index=true").mkString(",")
      spec mustEqual expected
    }

    // Example of fold...also can do more complex things like zipping to automatically build SFTs
    "work with foldLeft" in {
      val spec = ('a' to 'z').foldLeft(SchemaBuilder.builder()) { case (builder, name) =>
        builder.addString(name.toString)
      }

      val expected = ('a' to 'z').map(c => c.toString + ":" + "String").mkString(",")
      spec.spec mustEqual expected

      val sft = spec.build("foobar")
      sft.getAttributeCount mustEqual 26
      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual ('a' to 'z').map(_.toString)
    }

    "set default dtg correctly" in {
      SchemaBuilder.builder()
        .addDate("foobar", default = true)
        .build("foobar").getDtgField must beSome("foobar")

      SchemaBuilder.builder()
        .addDate("foobar")
        .userData(SimpleFeatureTypes.Configs.DEFAULT_DATE_KEY, "true")
        .build("foobar").getDtgField must beSome("foobar")

      SchemaBuilder.builder()
          .addDate("dtg", default = true)
          .addDate("foobar")
          .build("foobar").getDtgField must beSome("dtg")

      SchemaBuilder.builder()
        .addDate("dtg")
        .addDate("foobar", default = true)
        .build("foobar").getDtgField must beSome("foobar")
    }

    "build lists" in {
      val builder = SchemaBuilder.builder()
        .addList[Int]("i")
        .addList[Long]("l")
        .addList[Float]("f")
        .addList[Double]("d")
        .addList[String]("s")
        .addList[Date]("dt")
        .addList[UUID]("u")

      val expected =
        List(
          "i" -> "Integer",
          "l" -> "Long",
          "f" -> "Float",
          "d" -> "Double",
          "s" -> "String",
          "dt" -> "Date",
          "u" -> "UUID"
        ).map { case (k,v) => s"$k:List[$v]" }.mkString(",")

      builder.spec mustEqual expected

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 7
      sft.getAttributeDescriptors.asScala.forall(_.getType.getBinding must beAssignableFrom[java.util.List[AnyRef]])
    }


    "build lists with Java Types" in {
      val builder = SchemaBuilder.builder()
        .addList[java.lang.Integer]("i")
        .addList[java.lang.Long]("l")
        .addList[java.lang.Float]("f")
        .addList[java.lang.Double]("d")
        .addList[java.lang.String]("s")
        .addList[java.util.Date]("dt")
        .addList[java.util.UUID]("u")

      val expected =
        List(
          "i" -> "Integer", //for java use Integer instead of Int
          "l" -> "Long",
          "f" -> "Float",
          "d" -> "Double",
          "s" -> "String",
          "dt" -> "Date",
          "u" -> "UUID"
        ).map { case (k,v) => s"$k:List[$v]" }.mkString(",")

      builder.spec mustEqual expected

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 7
      sft.getAttributeDescriptors.asScala.forall(_.getType.getBinding must beAssignableFrom[java.util.List[_]])
    }

    "build maps" in {
      val builder = SchemaBuilder.builder()
        .addMap[Int,Int]("i")
        .addMap[Long,Long]("l")
        .addMap[Float,Float]("f")
        .addMap[Double,Double]("d")
        .addMap[String,String]("s")
        .addMap[Date,Date]("dt")
        .addMap[UUID,UUID]("u")

      val expected =
        List(
          "i" -> "Integer",
          "l" -> "Long",
          "f" -> "Float",
          "d" -> "Double",
          "s" -> "String",
          "dt" -> "Date",
          "u" -> "UUID"
        ).map { case (k,v) => s"$k:Map[$v,$v]" }.mkString(",")

      builder.spec mustEqual expected

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 7
      sft.getAttributeDescriptors.asScala.forall(_.getType.getBinding must beAssignableFrom[java.util.Map[_,_]])
    }

    "build maps of diff types" in {
      val builder = SchemaBuilder.builder()
        .addMap[Int,String]("a")
        .addMap[Long,UUID]("b")
        .addMap[Date,Float]("c")

      builder.spec mustEqual "a:Map[Integer,String],b:Map[Long,UUID],c:Map[Date,Float]"

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 3
      sft.getAttributeDescriptors.asScala.forall(_.getType.getBinding must beAssignableFrom[java.util.Map[_,_]])
    }

    "handle multiple geoms" in {
      val builder = SchemaBuilder.builder()
        .addMixedGeometry("geom")
        .addPoint("foobar", default = true)
        .addMultiLineString("mls")

      builder.spec mustEqual s"geom:Geometry:srid=4326,*foobar:Point:srid=4326,mls:MultiLineString:srid=4326"

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 3
      sft.getGeometryDescriptor.getLocalName mustEqual "foobar"
    }

    "handle Bytes type" in {
      val spec = SchemaBuilder.builder().addString("a").addBytes("b").spec
      spec mustEqual "a:String,b:Bytes"

      val lSpec = SchemaBuilder.builder().addList[Array[Byte]]("lst").spec
      lSpec mustEqual "lst:List[Bytes]"

      val mSpec = SchemaBuilder.builder().addMap[String,Array[Byte]]("m").spec
      mSpec mustEqual "m:Map[String,Bytes]"

      val m2Spec = SchemaBuilder.builder().addMap[Array[Byte],Array[Byte]]("m2").spec
      m2Spec mustEqual "m2:Map[Bytes,Bytes]"
    }

  }
}
