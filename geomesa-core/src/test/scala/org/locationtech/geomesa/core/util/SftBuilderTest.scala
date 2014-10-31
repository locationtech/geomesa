/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.core.util

import java.util.{Date, UUID}

import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.DigitSplitter
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SftBuilderTest extends Specification {

  sequential

  "SpecBuilder" >> {
    "build simple types" >> {
      val spec = new SftBuilder().intType("i").longType("l").floatType("f").doubleType("d").stringType("s").getSpec()
      spec mustEqual "i:Integer,l:Long,f:Float,d:Double,s:String"
    }

    "handle date and uuid types" >> {
      val spec = new SftBuilder().date("d").uuid("u").getSpec()
      spec mustEqual "d:Date,u:UUID"
    }

    "provide index when set to true" >> {
      val spec = new SftBuilder()
        .intType("i",true)
        .longType("l",true)
        .floatType("f",true)
        .doubleType("d",true)
        .stringType("s",true)
        .date("dt",true)
        .uuid("u",true)
        .getSpec()
      val expected = "i:Integer,l:Long,f:Float,d:Double,s:String,dt:Date,u:UUID".split(",").map(_+":index=true").mkString(",")
      spec mustEqual expected
    }

    "configure table splitters as strings" >> {
      val sft1 = new SftBuilder()
        .intType("i")
        .longType("l")
        .recordSplitter("org.locationtech.geomesa.core.data.DigitSplitter", Map("fmt" ->"%02d", "min" -> "0", "max" -> "99"))
        .build("test")

      // better - uses class directly (or at least less annoying)
      val sft2 = new SftBuilder()
        .intType("i")
        .longType("l")
        .recordSplitter(classOf[DigitSplitter], Map("fmt" ->"%02d", "min" -> "0", "max" -> "99"))
        .build("test")

      def test(sft: SimpleFeatureType) = {
        sft.getAttributeCount mustEqual 2
        sft.getAttributeDescriptors.map(_.getLocalName) must containAllOf(List("i", "l"))

        sft.getUserData.get(SimpleFeatureTypes.TABLE_SPLITTER) must be equalTo "org.locationtech.geomesa.core.data.DigitSplitter"
        val opts = sft.getUserData.get(SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS).asInstanceOf[Map[String, String]]
        opts.size must be equalTo 3
        opts("fmt") must be equalTo "%02d"
        opts("min") must be equalTo "0"
        opts("max") must be equalTo "99"
      }

      List(sft1, sft2) forall(test)
    }

    // Example of fold...also can do more complex things like zipping to automatically build SFTs
    "work with foldLeft" >> {
      val spec = ('a' to 'z').foldLeft(new SftBuilder()) { case (builder, name) =>
        builder.stringType(name.toString)
      }

      val expected = ('a' to 'z').map{ c => c.toString + ":" + "String" }.mkString(",")
      spec.getSpec() mustEqual expected

      val sft = spec.build("foobar")
      sft.getAttributeCount mustEqual 26
      sft.getAttributeDescriptors.map(_.getLocalName).toList mustEqual ('a' to 'z').map(_.toString).toList
    }

    "set default dtg correctly" >> {
      val sft = new SftBuilder()
        .date("foobar", false, true)
        .build("foobar")

      val sft2 = new SftBuilder()
        .date("foobar")
        .withDefaultDtg("foobar")
        .build("foobar")

      val sft3 = new SftBuilder()
        .date("foobar")
        .date("dtg")
        .withDefaultDtg("foobar")
        .build("foobar")

      val sft4 = new SftBuilder()
        .date("dtg")
        .date("foobar")
        .withDefaultDtg("foobar")
        .build("foobar")

      val sft5 = new SftBuilder()
        .date("dtg")
        .date("foobar", default=true)
        .build("foobar")

      List(sft, sft2, sft3, sft4, sft5) forall { _.getUserData.get(SF_PROPERTY_START_TIME) mustEqual "foobar" }
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

      builder.getSpec() mustEqual expected

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

      builder.getSpec() mustEqual expected

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

      builder.getSpec() mustEqual expected

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 7
      sft.getAttributeDescriptors.map(_.getType.getBinding).forall (_ must beAssignableFrom[java.util.Map[_,_]])
    }

    "build maps of diff types" >> {
      val builder = new SftBuilder()
        .mapType[Int,String]("a")
        .mapType[Long,UUID]("b")
        .mapType[Date,Float]("c")

      builder.getSpec() mustEqual "a:Map[Int,String],b:Map[Long,UUID],c:Map[Date,Float]"

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 3
      sft.getAttributeDescriptors.map(_.getType.getBinding).forall (_ must beAssignableFrom[java.util.Map[_,_]])
    }

    "handle multiple geoms" >> {
      val builder = new SftBuilder()
        .geometry("geom")
        .point("foobar", default = true)
        .multiLineString("mls", index=true)

      builder.getSpec() mustEqual "geom:Geometry:srid=4326,*foobar:Point:srid=4326:index=true,mls:MultiLineString:srid=4326:index=true"

      val sft = builder.build("foobar")
      sft.getAttributeCount mustEqual 3
      sft.getGeometryDescriptor.getLocalName mustEqual "foobar"
    }


  }
}
