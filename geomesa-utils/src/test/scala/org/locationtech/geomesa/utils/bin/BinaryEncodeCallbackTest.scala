/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.bin

import java.io.ByteArrayOutputStream

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.{ByteArrayCallback, ByteStreamCallback}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodedValues
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BinaryEncodeCallbackTest extends Specification {

  "BinaryEncodeCallback" should {

    "encode and decode simple attributes" in {
      val initial = EncodedValues(1200, 45.0f, 49.0f, System.currentTimeMillis(), -1L)
      ByteArrayCallback.apply(initial.trackId, initial.lat, initial.lon, initial.dtg)
      val encoded = ByteArrayCallback.result
      encoded must haveLength(16)
      val decoded = BinaryOutputEncoder.decode(encoded)
      decoded.lat mustEqual initial.lat
      decoded.lon mustEqual initial.lon
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual initial.trackId
    }

    "encode and decode optional simple attributes" in {
      val initial = EncodedValues(0, 45.0f, 49.0f, System.currentTimeMillis(), -1L)
      ByteArrayCallback.apply(initial.trackId, initial.lat, initial.lon, initial.dtg)
      val encoded = ByteArrayCallback.result
      encoded must haveLength(16)
      val decoded = BinaryOutputEncoder.decode(encoded)
      decoded.lat mustEqual initial.lat
      decoded.lon mustEqual initial.lon
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual initial.trackId
    }

    "encode and decode extended attributes" in {
      val initial = EncodedValues(1200, 45.0f, 49.0f, System.currentTimeMillis(), 10L)
      ByteArrayCallback.apply(initial.trackId, initial.lat, initial.lon, initial.dtg, initial.label)
      val encoded = ByteArrayCallback.result
      encoded must haveLength(24)
      val decoded = BinaryOutputEncoder.decode(encoded)
      decoded.lat mustEqual initial.lat
      decoded.lon mustEqual initial.lon
      Math.abs(decoded.dtg - initial.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decoded.trackId mustEqual initial.trackId
      decoded.label mustEqual initial.label
    }

    "encode and decode to an output stream" in {
      val time = System.currentTimeMillis()
      val one = EncodedValues(1200, 45.0f, 49.0f, time, 1000L)
      val two = EncodedValues(1201, 45.0f, 49.0f, time - 100, 3000L)
      val out = new ByteArrayOutputStream(48)
      val callback = new ByteStreamCallback(out)
      callback.apply(one.trackId, one.lat, one.lon, one.dtg, one.label)
      callback.apply(two.trackId, two.lat, two.lon, two.dtg, two.label)
      val array = out.toByteArray
      array must haveLength(48)

      val decodedOne = BinaryOutputEncoder.decode(array.splitAt(24)._1)
      decodedOne.lat mustEqual one.lat
      decodedOne.lon mustEqual one.lon
      Math.abs(decodedOne.dtg - one.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decodedOne.trackId mustEqual one.trackId
      decodedOne.label mustEqual one.label

      val decodedTwo = BinaryOutputEncoder.decode(array.splitAt(24)._2)
      decodedTwo.lat mustEqual two.lat
      decodedTwo.lon mustEqual two.lon
      Math.abs(decodedTwo.dtg - two.dtg) must beLessThan(1000L) // dates get truncated to nearest second
      decodedTwo.trackId mustEqual two.trackId
      decodedTwo.label mustEqual two.label
    }

    "encode faster to an output stream" in {
      skipped("integration")
      val times = 10000
      val one = EncodedValues(1200, 45.0f, 49.0f, System.currentTimeMillis(), 10000L)
      val out = new ByteArrayOutputStream(24 * times)
      val streamCallback = new ByteStreamCallback(out)

      // the first test run always takes a long time, even with some initialization...
      // flip the order to get a sense of how long each takes
      val start2 = System.currentTimeMillis()
      (0 to times).foreach(_ => ByteArrayCallback.apply(one.trackId, one.lat, one.lon, one.dtg, one.label))
      val total2 = System.currentTimeMillis() - start2
      println(s"array took $total2 ms")

      val start = System.currentTimeMillis()
      (0 to times).foreach(_ => streamCallback.apply(one.trackId, one.lat, one.lon, one.dtg, one.label))
      val total = System.currentTimeMillis() - start
      println(s"stream took $total ms")

      println
      success
    }
  }
}