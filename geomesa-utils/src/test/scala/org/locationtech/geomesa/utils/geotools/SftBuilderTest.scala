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
import org.locationtech.geomesa.utils.geotools.SftBuilder.Opts
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SftBuilderTest extends Specification {
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  sequential

  "SpecBuilder" >> {
    "build simple types" >> {
      val spec = new SftBuilder().intType("i").longType("l").floatType("f").doubleType("d").stringType("s").getSpec
      spec mustEqual "i:Integer,l:Long,f:Float,d:Double,s:String"
    }

    "handle date and uuid types" >> {
      val spec = new SftBuilder().date("d").uuid("u").getSpec
      spec mustEqual "d:Date,u:UUID"
    }

    "provide index when set to true" >> {
      val spec = new SftBuilder()
        .intType("i",    index = true)
        .longType("l",   index = true)
        .floatType("f",  index = true)
        .doubleType("d", index = true)
        .stringType("s", index = true)
        .date("dt",      Opts(index = true))
        .uuid("u",       index = true)
        .getSpec
      val expected = "i:Integer,l:Long,f:Float,d:Double,s:String,dt:Date,u:UUID".split(",").map(_+":index=true").mkString(",")
      spec mustEqual expected
    }

    // Example of fold...also can do more complex things like zipping to automatically build SFTs
    "work with foldLeft" >> {
      val spec = ('a' to 'z').foldLeft(new SftBuilder()) { case (builder, name) =>
        builder.stringType(name.toString)
      }

      val expected = ('a' to 'z').map{ c => c.toString + ":" + "String" }.mkString(",")
      spec.getSpec mustEqual expected

      val sft = spec.build("foobar")
      sft.getAttributeCount mustEqual 26
      sft.getAttributeDescriptors.map(_.getLocalName).toList mustEqual ('a' to 'z').map(_.toString).toList
    }

    "set default dtg correctly" >> {
      new SftBuilder()
        .date("foobar", default = true)
        .build("foobar").getDtgField must beSome("foobar")

      new SftBuilder()
        .date("foobar")
        .withDefaultDtg("foobar")
        .build("foobar").getDtgField must beSome("foobar")

      new SftBuilder()
        .date("foobar")
        .date("dtg")
        .withDefaultDtg("foobar")
        .build("foobar").getDtgField must beSome("foobar")

      new SftBuilder()
        .date("dtg")
        .date("foobar")
        .withDefaultDtg("foobar")
        .build("foobar").getDtgField must beSome("foobar")

      new SftBuilder()
        .date("dtg")
        .date("foobar", default = true)
        .build("foobar").getDtgField must beSome("foobar")
    }

    "build lists" >> {
      val builder = new SftBuilder()
        .listType[Int]("i")
        .listType[Long]("l")
        .listType[Float]("f")
        .listType[Double]("d")
        .listType[String]("s")
        .listType[Date]("dt")
        .listType[UUID]("u")

      val expected =
        List(
          "i" -> "Int",
          "l" -> "Long",
          "f" -> "Float",
          "d" -> "Double",
          "s" -> "String",
          "dt" -> "Date",
          "u" -> "UUID"
        ).map { case (k,v) => s"$k:List[$v]" }.mkString(",")

      builder.getSpec mustEqual expected

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 7
      sft.getAttributeDescriptors.map(_.getType.getBinding).forall (_ must beAssignableFrom[java.util.List[_]])
    }


    "build lists with Java Types" >> {
      val builder = new SftBuilder()
        .listType[java.lang.Integer]("i")
        .listType[java.lang.Long]("l")
        .listType[java.lang.Float]("f")
        .listType[java.lang.Double]("d")
        .listType[java.lang.String]("s")
        .listType[java.util.Date]("dt")
        .listType[java.util.UUID]("u")

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

      builder.getSpec mustEqual expected

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 7
      sft.getAttributeDescriptors.map(_.getType.getBinding).forall (_ must beAssignableFrom[java.util.List[_]])
    }

    "build maps" >> {
      val builder = new SftBuilder()
        .mapType[Int,Int]("i")
        .mapType[Long,Long]("l")
        .mapType[Float,Float]("f")
        .mapType[Double,Double]("d")
        .mapType[String,String]("s")
        .mapType[Date,Date]("dt")
        .mapType[UUID,UUID]("u")

      val expected =
        List(
          "i" -> "Int",
          "l" -> "Long",
          "f" -> "Float",
          "d" -> "Double",
          "s" -> "String",
          "dt" -> "Date",
          "u" -> "UUID"
        ).map { case (k,v) => s"$k:Map[$v,$v]" }.mkString(",")

      builder.getSpec mustEqual expected

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 7
      sft.getAttributeDescriptors.map(_.getType.getBinding).forall (_ must beAssignableFrom[java.util.Map[_,_]])
    }

    "build maps of diff types" >> {
      val builder = new SftBuilder()
        .mapType[Int,String]("a")
        .mapType[Long,UUID]("b")
        .mapType[Date,Float]("c")

      builder.getSpec mustEqual "a:Map[Int,String],b:Map[Long,UUID],c:Map[Date,Float]"

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 3
      sft.getAttributeDescriptors.map(_.getType.getBinding).forall (_ must beAssignableFrom[java.util.Map[_,_]])
    }

    "handle multiple geoms" >> {
      val builder = new SftBuilder()
        .geometry("geom")
        .point("foobar", default = true)
        .multiLineString("mls")

      builder.getSpec mustEqual s"geom:Geometry:srid=4326,*foobar:Point:srid=4326:index=true:$OPT_INDEX_VALUE=true,mls:MultiLineString:srid=4326"

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 3
      sft.getGeometryDescriptor.getLocalName mustEqual "foobar"
    }

    "handle Bytes type" >> {
      val spec = new SftBuilder().stringType("a").bytes("b").getSpec
      spec mustEqual "a:String,b:Bytes"

      val lSpec = new SftBuilder().listType[Array[Byte]]("lst").getSpec
      lSpec mustEqual "lst:List[Bytes]"

      val mSpec = new SftBuilder().mapType[String,Array[Byte]]("m").getSpec
      mSpec mustEqual "m:Map[String,Bytes]"

      val m2Spec = new SftBuilder().mapType[Array[Byte],Array[Byte]]("m2").getSpec
      m2Spec mustEqual "m2:Map[Bytes,Bytes]"
    }

  }
}
