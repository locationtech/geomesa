/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.utils.geotools

import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.`type`.AttributeDescriptor
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
  }
}
