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

  def givenBBox(bbox: BoundingBox): BoundingBox = BBOXCombiner.valueToBbox(BBOXCombiner.bboxToValue(bbox))
  def givenValue(v: Value): Value = BBOXCombiner.bboxToValue(BBOXCombiner.valueToBbox(v))

  "BBOXCombiner" should {
    "covert a number of BoundingBoxes to Values and back again" in {
      val bbox1 = BoundingBox(0, 10, 0, 10)
      val bbox2 = BoundingBox(0, 10, -10, 0)
      val bbox3 = BoundingBox(-10, 0, 0, 10)
      val bbox4 = BoundingBox(-10, 0, -10, 0)

      bbox1 must beEqualTo(givenBBox(bbox1))
      bbox2 must beEqualTo(givenBBox(bbox2))
      bbox3 must beEqualTo(givenBBox(bbox3))
      bbox4 must beEqualTo(givenBBox(bbox4))
    }

    "covert a number of Values to BoundingBoxes and back again" in {
      val value1 = new Value("POINT (0 0):POINT (10 10)".getBytes())
      val value2 = new Value("POINT (-10 -10):POINT (0 0)".getBytes())
      val value3 = new Value("POINT (-10 0):POINT (0 10)".getBytes())
      val value4 = new Value("POINT (0 -10):POINT (10 0)".getBytes())

      value1 must beEqualTo(givenValue(value1))
      value2 must beEqualTo(givenValue(value2))
      value3 must beEqualTo(givenValue(value3))
      value4 must beEqualTo(givenValue(value4))
    }
  }
}
