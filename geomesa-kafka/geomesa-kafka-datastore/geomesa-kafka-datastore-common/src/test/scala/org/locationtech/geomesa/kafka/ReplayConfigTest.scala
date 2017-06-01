/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.joda.time.{Duration, Instant}
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReplayConfigTest extends Specification with Mockito {


  "ReplayConfig" should {

    "throw an exception if start is after end" >> {

      val start = new Instant(19)
      val end = new Instant(15)
      val readBehind = Duration.millis(2)

      new ReplayConfig(start, end, readBehind) must throwA[IllegalArgumentException]
    }

    "allow start to equal end" >> {

      val start = new Instant(15)
      val end = new Instant(15)
      val readBehind = Duration.millis(2)

      val result = new ReplayConfig(start, end, readBehind)

      result.start mustEqual start
      result.end mustEqual end
      result.readBehind mustEqual readBehind
    }

    "calculate real start correctly" >> {

      val start = new Instant(10)
      val end = new Instant(20)
      val readBehind = Duration.millis(2)

      val result = new ReplayConfig(start, end, readBehind)

      result.realStartTime mustEqual new Instant(8)
    }
    
    "determine if before real start correctly" >> {

      val start = new Instant(10)
      val end = new Instant(20)
      val readBehind = Duration.millis(2)
      
      val result = new ReplayConfig(start, end, readBehind)
      
      result.isBeforeRealStart(mockMessage(7)) must beTrue
      result.isBeforeRealStart(mockMessage(8)) must beFalse
      result.isBeforeRealStart(mockMessage(9)) must beFalse
      result.isBeforeRealStart(mockMessage(10)) must beFalse
    }

    "determine if not after end correctly" >> {

      val start = new Instant(10)
      val end = new Instant(20)
      val readBehind = Duration.millis(2)

      val result = new ReplayConfig(start, end, readBehind)

      result.isNotAfterEnd(mockMessage(19)) must beTrue
      result.isNotAfterEnd(mockMessage(20)) must beTrue
      result.isNotAfterEnd(mockMessage(21)) must beFalse
    }

    "determine if in windown correctly" >> {

      val start = new Instant(10)
      val end = new Instant(20)
      val readBehind = Duration.millis(2)

      val result = new ReplayConfig(start, end, readBehind)

      // before read behind
      result.isInWindow(7) must beFalse

      // read behind
      result.isInWindow(8) must beFalse
      result.isInWindow(9) must beFalse

      // window
      (10 to 20).forall {i =>
        result.isInWindow(i) must beTrue
      }

      // after
      result.isInWindow(21) must beFalse
      result.isInWindow(22) must beFalse
    }

    "encode should encode correctly" >> {

      val start = new Instant(1234)
      val end = new Instant(4321)
      val readBehind = Duration.millis(1024)

      val rc = new ReplayConfig(start, end, readBehind)

      ReplayConfig.encode(rc) mustEqual s"4d2-10e1-400"
    }

    "decode should decode correctly" >> {

      val start = new Instant(1234)
      val end = new Instant(4321)
      val readBehind = Duration.millis(1024)
      val rc = new ReplayConfig(start, end, readBehind)
      val encoded = ReplayConfig.encode(rc)

      ReplayConfig.decode(encoded) must beSome(rc)
    }
  }

  def mockMessage(i: Long): GeoMessage = {
    val msg = mock[GeoMessage]
    msg.timestamp returns new Instant(i)
    msg
  }
}
