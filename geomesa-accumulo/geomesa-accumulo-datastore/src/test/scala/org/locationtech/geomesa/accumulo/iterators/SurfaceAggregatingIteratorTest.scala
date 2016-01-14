/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

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
