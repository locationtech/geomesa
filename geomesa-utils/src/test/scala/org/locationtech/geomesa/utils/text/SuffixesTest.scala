/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SuffixesTest extends Specification {

  "Suffixes.Time" should {
    "parse various units" >> {
      import Suffixes.Time._
      days("1d").get mustEqual 1
      days("30d").get mustEqual 30

      hours("1h").get mustEqual 1
      hours("24h").get mustEqual 24
      hours("120m").get mustEqual 2
      hours("30d").get mustEqual 30*24

      millis("30s").get mustEqual 30*1000
      millis("5m").get mustEqual 5*60*1000

      millis("5000").get mustEqual 5000L
    }
  }

  "Suffixes.Memory" should {
    import Suffixes.Memory._

    "parse various units to bytes" >> {
      bytes("1000").get mustEqual 1000
      bytes("1b").get mustEqual 1
      bytes("123456B").get mustEqual 123456

      bytes("1k").get mustEqual 1024
      bytes("1K").get mustEqual 1024
      bytes("1kb").get mustEqual 1024
      bytes("1Kb").get mustEqual 1024

      bytes("1m").get mustEqual 1024*1024
      bytes("200m").get mustEqual 1024*1024*200
      bytes("300MB").get mustEqual 1024*1024*300

      bytes("7G").get mustEqual 1024L*1024L*1024L*7

      bytes(s"${Long.MaxValue}").get mustEqual 9223372036854775807l
    }

    "return none on overflow" >> {
      //java.Long.MAX_VALUE =  9223372036854775807
      val bigNum            = "9999999999999999999"
      bytes(bigNum) must beNone
      bytes(s"${Long.MaxValue}k") must beNone
    }
  }
}
