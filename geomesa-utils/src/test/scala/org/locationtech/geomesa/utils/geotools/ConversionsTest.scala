/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.Date

import org.locationtech.jts.geom.Geometry
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.feature.collection.{FeatureIteratorImpl, SimpleFeatureIteratorImpl}
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.feature.{DefaultFeatureCollection, FeatureIterator}
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ConversionsTest extends Specification with Mockito {

  sequential

  "RichSimpleFeature" should {

    import Conversions.RichSimpleFeature

    val sf = mock[SimpleFeature]

    "support implicit conversion" >> {
      val rsf: RichSimpleFeature = sf
      success
    }


    "be able to access default geometry" >> {
      val geo = mock[Geometry]
      sf.getDefaultGeometry returns geo

      sf.geometry mustEqual geo
    }

    "throw exception if defaultgeometry is not a Geometry" >> {
      sf.getDefaultGeometry returns "not a Geometry!"

      sf.geometry must throwA[ClassCastException]
    }

    "provide type safe access to user data" >> {

      val expected: Integer = 5

      val userData = Map[AnyRef, AnyRef]("key" -> expected).asJava
      sf.getUserData returns userData

      "when type is correct" >> {

        val result = sf.userData[Integer]("key")
        result must beSome(expected)
      }

      "or none when type is not correct" >> {

        val result = sf.userData[String]("key")
        result must beNone
      }

      "or none when value does not exist" >> {

        val result: Option[String] = sf.userData[String]("foo")
        result must beNone
      }
    }
  }

  "RichSimpleFeatureType" should {

    import RichSimpleFeatureType.RichSimpleFeatureType

    def newSft = SimpleFeatureTypes.createType("test", "dtg:Date,*geom:Point:srid=4326")
    "support implicit conversion" >> {
      val sft = newSft
      val rsft: RichSimpleFeatureType = sft
      success
    }

    "set and get table sharing boolean" >> {
      val sft = newSft
      sft.setTableSharing(true)
      sft.isTableSharing must beTrue
    }

    "provide type safe access to user data" >> {

      val expected: Integer = 5

      val sft = newSft
      sft.getUserData.put("key", expected)

      "when type is correct" >> {
        val result = sft.userData[Integer]("key")
        result must beSome(expected)
      }

      "or none when value does not exist" >> {
        val result: Option[String] = sft.userData[String]("foo")
        result must beNone
      }
    }

  }
}
