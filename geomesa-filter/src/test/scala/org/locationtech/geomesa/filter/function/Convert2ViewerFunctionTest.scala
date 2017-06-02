/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import java.io.ByteArrayOutputStream

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Convert2ViewerFunctionTest extends Specification {

  "Convert2ViewerFunction" should {

    "encode and decode simple attributes" in {
      val initial = BasicValues(45.0f, 49.0f, System.currentTimeMillis(), 1200)
      val encoded = Convert2ViewerFunction.encodeToByteArray(initial)
      encoded must haveLength(16)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded.lat mustEqual initial.lat
      decoded.lon mustEqual initial.lon
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual initial.trackId
    }

    "encode and decode optional simple attributes" in {
      val initial = BasicValues(45.0f, 49.0f, System.currentTimeMillis(), 0)
      val encoded = Convert2ViewerFunction.encodeToByteArray(initial)
      encoded must haveLength(16)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded.lat mustEqual initial.lat
      decoded.lon mustEqual initial.lon
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual initial.trackId
    }

    "encode and decode extended attributes" in {
      val initial = ExtendedValues(45.0f,
                                   49.0f,
                                   System.currentTimeMillis(),
                                   1200,
                                   10)
      val encoded = Convert2ViewerFunction.encodeToByteArray(initial)
      encoded must haveLength(24)
      val decoded = Convert2ViewerFunction.decode(encoded)
      decoded must beAnInstanceOf[ExtendedValues]
      decoded.lat mustEqual initial.lat
      decoded.lon mustEqual initial.lon
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual initial.trackId
      decoded.asInstanceOf[ExtendedValues].label mustEqual initial.label
    }

    "encode and decode to an output stream" in {
      val time = System.currentTimeMillis()
      val one = ExtendedValues(45.0f, 49.0f, time, 1200, 1000)
      val two = ExtendedValues(45.0f, 49.0f, time - 100, 1201, 3000)
      val out = new ByteArrayOutputStream(48)
      Convert2ViewerFunction.encode(one, out)
      Convert2ViewerFunction.encode(two, out)
      val array = out.toByteArray
      array must haveLength(48)

      val decodedOne = Convert2ViewerFunction.decode(array.splitAt(24)._1)
      decodedOne must beAnInstanceOf[ExtendedValues]
      decodedOne.lat mustEqual one.lat
      decodedOne.lon mustEqual one.lon
      Math.abs(decodedOne.dtg - one.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decodedOne.trackId mustEqual one.trackId
      decodedOne.asInstanceOf[ExtendedValues].label mustEqual one.label

      val decodedTwo = Convert2ViewerFunction.decode(array.splitAt(24)._2)
      decodedTwo must beAnInstanceOf[ExtendedValues]
      decodedTwo.lat mustEqual two.lat
      decodedTwo.lon mustEqual two.lon
      Math.abs(decodedTwo.dtg - two.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decodedTwo.trackId mustEqual two.trackId
      decodedTwo.asInstanceOf[ExtendedValues].label mustEqual two.label
    }

    "encode faster to an output stream" in {
      skipped("integration")
      val times = 10000
      val one = ExtendedValues(45.0f, 49.0f, System.currentTimeMillis(), 1200, 10000)
      val out = new ByteArrayOutputStream(24 * times)

      // the first test run always takes a long time, even with some initialization...
      // flip the order to get a sense of how long each takes
      val start2 = System.currentTimeMillis()
      (0 to times).foreach(_ => Convert2ViewerFunction.encodeToByteArray(one))
      val total2 = System.currentTimeMillis() - start2
      println(s"array took $total2 ms")

      val start = System.currentTimeMillis()
      (0 to times).foreach(_ => Convert2ViewerFunction.encode(one, out))
      val total = System.currentTimeMillis() - start
      println(s"stream took $total ms")

      println
      success
    }
  }
}