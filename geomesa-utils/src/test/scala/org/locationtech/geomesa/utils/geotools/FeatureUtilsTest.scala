/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FeatureUtilsTest extends Specification {

  "Feature Utils" should {

    "preserve user data when re-typing a simple feature type" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point,dtg:Date")
      sft.getUserData.put("foo", "bar")
      sft.getUserData.put("key", "val")


      val result = FeatureUtils.retype(sft, Array("geom", "name"))

      result.getTypeName mustEqual "test"
      result.getAttributeCount mustEqual 2
      result.getDescriptor(0) mustEqual sft.getDescriptor(1)
      result.getDescriptor(1) mustEqual sft.getDescriptor(0)
      result.getUserData mustEqual sft.getUserData
    }

    "preserve user data when re-typing a simple feature" >> {

      val origType = SimpleFeatureTypes.createType("test", "name:String,*geom:Point,dtg:Date")
      origType.getUserData.put("type-key", "type-val")

      val reType = FeatureUtils.retype(origType, Array("geom", "name"))

      val values = Array[AnyRef]("id0", WKTUtils.read("POINT(-110 30)"), "2012-01-02T05:06:07.000Z")
      val sf = SimpleFeatureBuilder.build(origType, values, "id0")
      sf.getUserData.put("feature-key", "feature-val")

      val result = FeatureUtils.retype(sf, reType)

      result.getFeatureType mustEqual reType
      result.getAttributeCount mustEqual 2
      result.getAttribute(0) mustEqual sf.getAttribute(1)
      result.getAttribute(1) mustEqual sf.getAttribute(0)
      result.getDefaultGeometry mustEqual sf.getDefaultGeometry
      result.getUserData mustEqual sf.getUserData
    }

    "preserve user data when re-building a simple feature type" >> {

      val sft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point,dtg:Date")
      sft.getUserData.put("foo", "bar")
      sft.getUserData.put("key", "val")

      val builder = FeatureUtils.builder(sft)
      builder.add("index", classOf[Int])

      val result = builder.buildFeatureType()

      result.getTypeName mustEqual "test"
      result.getAttributeCount mustEqual 4
      result.getDescriptor(0) mustEqual sft.getDescriptor(0)
      result.getDescriptor(1) mustEqual sft.getDescriptor(1)
      result.getDescriptor(2) mustEqual sft.getDescriptor(2)
      result.getDescriptor(3).getLocalName mustEqual "index"
      result.getDescriptor(3).getType.getBinding mustEqual classOf[Int]
      result.getUserData mustEqual sft.getUserData
    }

    "correctly fail to identify reserved words in a valid simple feature type" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point,dtg:Date")
      val result = FeatureUtils.sftReservedWords(sft)
      result must beEmpty
    }

    "correctly fail to identify reserved words in a valid simple feature type with user data" >> {
      val sft = SimpleFeatureTypes.createType(
        "test", "an_id:Integer,map:Map[String,Integer],dtg:Date,geom:Geometry:srid=4326;geomesa.mixed.geometries=true")
      val result = FeatureUtils.sftReservedWords(sft)
      result must beEmpty
    }

    "correctly fail to identify reserved words in a valid, multi-line simple feature type" >> {
      val sft = SimpleFeatureTypes.createType(
        "test",
        """
          |names:List[String],
          |fingers:List[String],
          |skills:Map[String,Integer],
          |metadata:Map[Double,String],
          |dtg:Date,
          |*geom:Point:srid=4326
        """.stripMargin)
      val result = FeatureUtils.sftReservedWords(sft)
      result must beEmpty
    }

    "correctly identify a single reserved word in an invalid simple feature type" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,id:String,*geom:Point,dtg:Date")
      val result = FeatureUtils.sftReservedWords(sft)
      result must containTheSameElementsAs(Seq("ID"))
    }

    "correctly identify multiple reserved words in an invalid simple feature type" >> {
      val sft = SimpleFeatureTypes.createType("test", "id:String,name:String,*point:Point,dtg:Date")
      val result = FeatureUtils.sftReservedWords(sft)
      result must containTheSameElementsAs(Seq("ID", "POINT"))
    }
  }
}
