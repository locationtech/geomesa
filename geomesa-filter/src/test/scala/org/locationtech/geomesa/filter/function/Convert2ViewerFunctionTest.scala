/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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


package org.locationtech.geomesa.filter.function

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Convert2ViewerFunctionTest extends Specification {

  "Convert2ViewerFunction" should {

    "encode and decode simple attributes" in {
      val initial = BasicValues(45.0f, 49.0f, System.currentTimeMillis(), Some("1200"))
      val encoded = Convert2ViewerFunction.encode(initial)
      encoded must haveLength(16)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded.lat mustEqual(initial.lat)
      decoded.lon mustEqual(initial.lon)
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual(initial.trackId)
    }

    "encode and decode optional simple attributes" in {
      val initial = BasicValues(45.0f, 49.0f, System.currentTimeMillis(), None)
      val encoded = Convert2ViewerFunction.encode(initial)
      encoded must haveLength(16)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded.lat mustEqual(initial.lat)
      decoded.lon mustEqual(initial.lon)
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual(initial.trackId)
    }

    "encode and decode extended attributes" in {
      val initial = ExtendedValues(45.0f,
                                   49.0f,
                                   System.currentTimeMillis(),
                                   Some("1200"),
                                   Some("label"))
      val encoded = Convert2ViewerFunction.encode(initial)
      encoded must haveLength(24)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded must beAnInstanceOf[ExtendedValues]
      decoded.lat mustEqual(initial.lat)
      decoded.lon mustEqual(initial.lon)
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual(initial.trackId)
      decoded.asInstanceOf[ExtendedValues].label mustEqual(initial.label)
    }

    "truncate long labels" in {
      val initial = ExtendedValues(45.0f,
                                   49.0f,
                                   System.currentTimeMillis(),
                                   Some("track that is too long"),
                                   Some("label that is too long"))
      val encoded = Convert2ViewerFunction.encode(initial)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded must beAnInstanceOf[ExtendedValues]
      decoded.lat mustEqual(initial.lat)
      decoded.lon mustEqual(initial.lon)
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId.get mustEqual(initial.trackId.get.substring(0, 4))
      decoded.asInstanceOf[ExtendedValues].label.get mustEqual(initial.label.get.substring(0, 8))
    }
  }
}