/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.factory.Hints
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TypeUpdatingFeatureReaderTest extends Specification with Mockito {

  val sftName = "TypeUpdatingFeatureReaderTest"
  val sft = SimpleFeatureTypes.createType(sftName, "name:String,*geom:Point,dtg:Date")

  def getDelegate(featureCount: Int): FR = getDelegate(getFeatures(featureCount))
  def getDelegate(features: Seq[SimpleFeature]): FR = new DFR(sft, new DFI(features.iterator))

  def getFeatures(count: Int) = (0 until count).map { i =>
    val values = Seq[AnyRef](i.toString, WKTUtils.read("POINT(-110 30)"), "2012-01-02T05:06:07.000Z")
    val sf = SimpleFeatureBuilder.build(sft, values, i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }

  "TypeUpdatingFeatureReader" should {

    "provide the re-type" >> {
      val delegateReader = mock[FR]
      val delegateFT = mock[SimpleFeatureType]
      delegateReader.getFeatureType returns delegateFT

      val reader = new TypeUpdatingFeatureReader(delegateReader, sft)
      val result = reader.getFeatureType

      result mustEqual sft
    }

    "provide the delegate" >> {
      val delegateReader = mock[FR]

      val reader = new TypeUpdatingFeatureReader(delegateReader, sft)
      val result = reader.getDelegate

      result mustEqual delegateReader
    }

    "delegate hasNext" >> {
      val delegateReader = getDelegate(2)
      val reader = new TypeUpdatingFeatureReader(delegateReader, sft)

      reader.hasNext must beTrue
      reader.next()
      reader.hasNext must beTrue
      reader.next()
      reader.hasNext must beFalse
    }

    "delegate close" >> {
      val delegate = mock[FR]

      val reader = new TypeUpdatingFeatureReader(delegate, sft)
      reader.close()

      there was one(delegate).close
    }

    "next must re-type and preserve user data" >> {
      val features = getFeatures(1)
      val expected = features.head

      "with a single attribute" >> {
        val delegateReader = getDelegate(features)
        val targetType = SimpleFeatureTypeBuilder.retype(sft, Seq("geom"))

        val reader = new TypeUpdatingFeatureReader(delegateReader, targetType)
        val result = reader.next()

        result must not(beNull)
        result.getAttributeCount mustEqual 1
        result.getAttribute(0) mustEqual expected.getAttribute("geom")
        result.getAttribute("geom") mustEqual expected.getAttribute("geom")
        result.getUserData mustEqual expected.getUserData
      }

      "with multiple attributes" >> {
        val delegateReader = getDelegate(features)
        val targetType = SimpleFeatureTypeBuilder.retype(sft, Seq("geom", "dtg"))

        val reader = new TypeUpdatingFeatureReader(delegateReader, targetType)
        val result = reader.next()

        result must not(beNull)
        result.getAttributeCount mustEqual 2

        result.getAttribute(0) mustEqual expected.getAttribute("geom")
        result.getAttribute("geom") mustEqual expected.getAttribute("geom")

        result.getAttribute(1) mustEqual expected.getAttribute("dtg")
        result.getAttribute("dtg") mustEqual expected.getAttribute("dtg")

        result.getUserData mustEqual expected.getUserData
      }
    }

  }
}
