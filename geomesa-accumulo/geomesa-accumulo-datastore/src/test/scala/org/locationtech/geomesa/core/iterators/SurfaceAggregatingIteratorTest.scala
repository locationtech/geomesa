/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.iterators

import java.nio.ByteBuffer

import org.apache.accumulo.core.data.{Key, Value}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SurfaceAggregatingIteratorTest extends Specification with IteratorTest {

  def getIterator(gh1: String, gh2: String, precision: Int) = {
    val iter = new SurfaceAggregatingIterator()
    val aggOpt = AggregatingKeyIterator.aggOpt
    iter.setOpt(aggOpt + iter.bottomLeft, gh1)
    iter.setOpt(aggOpt + iter.topRight, gh2)
    iter.setOpt(aggOpt + "precision", precision.toString)
    iter.setOpt(aggOpt + "dims", "16,16")
    iter
  }

  "SurfaceAggregatingIterator" should {
    "properly initialize corners" in {
      val gh1 = "dq00000"
      val gh2 = "dq0000z"
      val precision = 35
      val iter = getIterator(gh1, gh2, precision)
      iter.bbox.ll.getX mustEqual GeoHash(gh1, precision).x
      iter.bbox.ur.getY mustEqual GeoHash(gh2, precision).y
    }
  }

  "SurfaceAggregatingIterator" should {
    "work if given args: bottomLeft->dq00000, topRight-> dq0000z, precision -> 35" in {
      val gh1 = "dq00000"
      val gh2 = "dq0000z"
      val precision = 35
      val iter = getIterator(gh1, gh2, precision)

      iter.collect(new Key(gh1), new Value(ByteBuffer.allocate(8).putDouble(.4).array()))
      iter.collect(new Key(gh2), new Value(ByteBuffer.allocate(8).putDouble(.6).array()))
      iter.aggregate.get()(15) & 0xff mustEqual 153
      iter.aggregate.get()(240) & 0xff mustEqual 102
    }
  }

  "SurfaceAggregatingIterator" should {
    "work if given args: bottomLeft->dq00000, topRight-> dq00qz, precision -> 33" in {
      val gh1 = "dq00000"
      val gh2 = "dq000qz"
      val precision = 33
      val iter = getIterator(gh1, gh2, precision)

      iter.collect(new Key(gh1), new Value(ByteBuffer.allocate(8).putDouble(.4).array()))
      iter.collect(new Key(gh2), new Value(ByteBuffer.allocate(8).putDouble(.6).array()))
      iter.aggregate.get()(15) & 0xff mustEqual 153
      iter.aggregate.get()(240) & 0xff mustEqual 102
    }
  }
}
