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
package org.locationtech.geomesa.kafka

import java.util.Date

import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.{Filter, PropertyIsEqualTo}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReplayTimeHelperTest extends Specification with Mockito {

  "ReplayTimeHelper" should {

    "addReplayTimeAttribute to a SFT builder" >> {
      val builder = new SimpleFeatureTypeBuilder
      builder.setName("test")

      ReplayTimeHelper.addReplayTimeAttribute(builder)

      val sft = builder.buildFeatureType()

      sft.getDescriptor(0).getLocalName mustEqual ReplayTimeHelper.AttributeName
      sft.getDescriptor(0).getType.getBinding mustEqual classOf[java.util.Date]
    }

    "be able to create a Filter from an Instant" >> {
      val time = 12345L
      val instant = new Instant(time)
      val date = new Date(time)

      val result: Filter = ReplayTimeHelper.toFilter(instant)
      result must beAnInstanceOf[PropertyIsEqualTo]

      val piet = result.asInstanceOf[PropertyIsEqualTo]

      piet.getExpression1 must beAnInstanceOf[PropertyName]
      piet.getExpression1.asInstanceOf[PropertyName].getPropertyName mustEqual ReplayTimeHelper.AttributeName

      piet.getExpression2 must beAnInstanceOf[Literal]
      piet.getExpression2.asInstanceOf[Literal].getValue mustEqual date
    }

    "be able to add log time to a simple feature" >> {
      val time = 12345L
      val date = new Date(time)

      val sft = {
        val builder = FeatureUtils.builder(KafkaConsumerTestData.sft)
        ReplayTimeHelper.addReplayTimeAttribute(builder)
        builder.buildFeatureType()
      }

      val helper = new ReplayTimeHelper(sft, time)

      val sf = KafkaConsumerTestData.track0v0
      val result: SimpleFeature = helper.addReplayTime(sf)
      result.getAttributeCount mustEqual sf.getAttributeCount + 1

      sf.getAttribute(ReplayTimeHelper.AttributeName) must beNull
      result.getAttribute(ReplayTimeHelper.AttributeName) mustEqual date
    }

  }
}
