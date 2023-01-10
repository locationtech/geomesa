/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ConversionsTest extends Specification with Mockito {

  def newSft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")

  "RichSimpleFeature" should {

    import Conversions.RichSimpleFeature

    "support implicit conversion" >> {
      val sf = mock[SimpleFeature]
      val rsf: RichSimpleFeature = sf
      success
    }


    "be able to access default geometry" >> {
      val sf = mock[SimpleFeature]
      val geo = mock[Geometry]
      sf.getDefaultGeometry returns geo
      sf.geometry mustEqual geo
    }

    "throw exception if defaultgeometry is not a Geometry" >> {
      val sf = mock[SimpleFeature]
      sf.getDefaultGeometry returns "not a Geometry!"
      sf.geometry must throwA[ClassCastException]
    }

    "provide type safe access to user data" >> {
      val sf = new SimpleFeatureImpl(List[AnyRef](null, null, null).asJava, newSft, new FeatureIdImpl(""))
      sf.getUserData.put("key", Int.box(5))

      "when type is correct" >> {
        sf.userData[Integer]("key") must beSome(Int.box(5))
      }

      "or none when type is not correct" >> {
        sf.userData[String]("key") must beNone
      }

      "or none when value does not exist" >> {
        sf.userData[String]("foo") must beNone
      }
    }
  }

  "RichSimpleFeatureType" should {

    import RichSimpleFeatureType.RichSimpleFeatureType

    "support implicit conversion" >> {
      val sft = newSft
      val rsft: RichSimpleFeatureType = sft
      success
    }

    "set and get table sharing boolean" >> {
      val sft = newSft
      sft.getUserData.put(SimpleFeatureTypes.Configs.TableSharing, "true")
      sft.isTableSharing must beTrue
    }

    "provide type safe access to user data" >> {

      val sft = newSft
      sft.getUserData.put("key", Int.box(5))

      "when type is correct" >> {
        sft.userData[Integer]("key") must beSome(Int.box(5))
      }

      "or none when value does not exist" >> {
        sft.userData[String]("foo") must beNone
      }
    }
  }

  "RichAttributeDescriptor" should {

    import RichAttributeDescriptors.RichAttributeDescriptor

    "provide case-insensitive boolean matches" >> {
      foreach(Seq("TRUE", "true", "True")) { b =>
        val sft = newSft
        sft.getDescriptor("name").getUserData.put("json", b)
        sft.getDescriptor("name").isJson must beTrue
      }
    }
  }
}
