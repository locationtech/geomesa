/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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
      hours("120min").get mustEqual 2
      hours("30d").get mustEqual 30*24

      millis("30s").get mustEqual 30*1000
      millis("5min").get mustEqual 5*60*1000

      millis("5000").get mustEqual 5000L
    }
  }

  "Suffixes.Memory" should {
    import Suffixes.Memory._

    "parse various units to bytes" >> {
      bytes("1000") must beASuccessfulTry(1000L)
      bytes("1b") must beASuccessfulTry(1L)
      bytes("123456B") must beASuccessfulTry(123456L)

      bytes("1k") must beASuccessfulTry(1024L)
      bytes("1K") must beASuccessfulTry(1024L)
      bytes("1kb") must beASuccessfulTry(1024L)
      bytes("1Kb") must beASuccessfulTry(1024L)

      bytes("1m") must beASuccessfulTry(1024L*1024L)
      bytes("200m") must beASuccessfulTry(1024L*1024L*200L)
      bytes("300MB") must beASuccessfulTry(1024L*1024L*300L)

      bytes("7G") must beASuccessfulTry(1024L*1024L*1024L*7)

      bytes(s"${Long.MaxValue}") must beASuccessfulTry(9223372036854775807L)
    }

    "return none on overflow" >> {
      //java.Long.MAX_VALUE =  9223372036854775807
      val bigNum            = "9999999999999999999"
      bytes(bigNum) must beAFailedTry
      bytes(s"${Long.MaxValue}k") must beAFailedTry
    }
  }
}
