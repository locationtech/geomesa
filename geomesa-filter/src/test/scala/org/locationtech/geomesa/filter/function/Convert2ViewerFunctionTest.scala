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

import java.io.ByteArrayOutputStream

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Convert2ViewerFunctionTest extends Specification {

  "Convert2ViewerFunction" should {

    "encode and decode simple attributes" in {
      val initial = BasicValues(45.0f, 49.0f, System.currentTimeMillis(), Some("1200"))
      val encoded = Convert2ViewerFunction.encodeToByteArray(initial)
      encoded must haveLength(16)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded.lat mustEqual(initial.lat)
      decoded.lon mustEqual(initial.lon)
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId.get.toInt mustEqual(initial.trackId.get.hashCode)
    }

    "encode and decode optional simple attributes" in {
      val initial = BasicValues(45.0f, 49.0f, System.currentTimeMillis(), None)
      val encoded = Convert2ViewerFunction.encodeToByteArray(initial)
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
      val encoded = Convert2ViewerFunction.encodeToByteArray(initial)
      encoded must haveLength(24)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded must beAnInstanceOf[ExtendedValues]
      decoded.lat mustEqual(initial.lat)
      decoded.lon mustEqual(initial.lon)
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId.get.toInt mustEqual(initial.trackId.get.hashCode)
      decoded.asInstanceOf[ExtendedValues].label mustEqual(initial.label)
    }

    "truncate long labels" in {
      val initial = ExtendedValues(45.0f,
                                   49.0f,
                                   System.currentTimeMillis(),
                                   Some("track that is too long"),
                                   Some("label that is too long"))
      val encoded = Convert2ViewerFunction.encodeToByteArray(initial)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded must beAnInstanceOf[ExtendedValues]
      decoded.lat mustEqual(initial.lat)
      decoded.lon mustEqual(initial.lon)
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId.get.toInt mustEqual(initial.trackId.get.hashCode)
      decoded.asInstanceOf[ExtendedValues].label.get mustEqual(initial.label.get.substring(0, 8))
    }

    "encode and decode to an output stream" in {
      val time = System.currentTimeMillis()
      val one = ExtendedValues(45.0f,
        49.0f,
        time,
        Some("1200"),
        Some("label"))
      val two = ExtendedValues(45.0f,
        49.0f,
        time - 100,
        Some("1201"),
        Some("label2"))
      val out = new ByteArrayOutputStream(48)
      Convert2ViewerFunction.encode(one, out)
      Convert2ViewerFunction.encode(two, out)
      val array = out.toByteArray
      array must haveLength(48)

      val decodedOne = Convert2ViewerFunction.decode(array.splitAt(24)._1)
      decodedOne must beAnInstanceOf[ExtendedValues]
      decodedOne.lat mustEqual(one.lat)
      decodedOne.lon mustEqual(one.lon)
      Math.abs(decodedOne.dtg - one.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decodedOne.trackId.get.toInt mustEqual(one.trackId.get.hashCode)
      decodedOne.asInstanceOf[ExtendedValues].label mustEqual(one.label)

      val decodedTwo = Convert2ViewerFunction.decode(array.splitAt(24)._2)
      decodedTwo must beAnInstanceOf[ExtendedValues]
      decodedTwo.lat mustEqual(two.lat)
      decodedTwo.lon mustEqual(two.lon)
      Math.abs(decodedTwo.dtg - two.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decodedTwo.trackId.get.toInt mustEqual(two.trackId.get.hashCode)
      decodedTwo.asInstanceOf[ExtendedValues].label mustEqual(two.label)
    }
  }
}