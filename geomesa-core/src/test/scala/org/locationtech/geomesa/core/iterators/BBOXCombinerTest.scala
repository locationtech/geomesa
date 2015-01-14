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

package org.locationtech.geomesa.core.iterators

import org.apache.accumulo.core.data.Value
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BBOXCombinerTest extends Specification {
  sequential

  "BBOXCombiner" should {

    "covert a BoundingBox to a Value" in {
      val bbox = BoundingBox(0, 10, 0, 10)

      val value = BBOXCombiner.bboxToValue(bbox)

      value must beAnInstanceOf[Value]
    }

    "convert a Value into a BoundingBox" in {
      val value = new Value("POINT (0 0):POINT (10 10)".getBytes())

      val bbox = BBOXCombiner.valueToBbox(value)

      bbox must beAnInstanceOf[BoundingBox]
    }

    "convert a BoundingBox to a Value and then back again into a BoundingBox" in {
      val bbox = BoundingBox(0, 10, 0, 10)

      val value = BBOXCombiner.bboxToValue(bbox)

      val resbbox = BBOXCombiner.valueToBbox(value)

      bbox mustEqual(resbbox)
    }

  }

}
