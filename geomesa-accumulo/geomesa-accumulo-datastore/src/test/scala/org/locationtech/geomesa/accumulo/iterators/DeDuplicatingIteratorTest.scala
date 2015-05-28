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


package org.locationtech.geomesa.accumulo.iterators

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeDuplicatingIteratorTest extends Specification {

  "DeDuplicatingIterator" should {
    "filter on unique elements" in {
      val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")
      val attributes = Array[AnyRef]("POINT(0,0)")
      val features = Seq(
        new ScalaSimpleFeature("1", sft, attributes),
        new ScalaSimpleFeature("2", sft, attributes),
        new ScalaSimpleFeature("0", sft, attributes),
        new ScalaSimpleFeature("1", sft, attributes),
        new ScalaSimpleFeature("3", sft, attributes),
        new ScalaSimpleFeature("4", sft, attributes),
        new ScalaSimpleFeature("1", sft, attributes),
        new ScalaSimpleFeature("3", sft, attributes)
      )
      val deduped = new DeDuplicatingIterator(features.toIterator).toSeq
      deduped must haveLength(5)
      deduped.map(_.getID) mustEqual Seq("1", "2", "0", "3", "4")
    }
  }
}