/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SoftThreadLocalTest extends Specification {

  "SoftThreadLocalCache" should {

    "allow the value to be put" >> {
      val cache = new SoftThreadLocal[String]()
      cache.put("value")
      cache.cache.get().get must beSome("value")
    }

    "allow the value to be retreived" >> {
      val cache = new SoftThreadLocal[String]()
      cache.put("value")
      cache.get must beSome("value")
    }

    "allow the value to be cleared" >> {
      val cache = new SoftThreadLocal[String]()
      cache.put("value")
      cache.clear()
      cache.get must beNone
    }

    "allow getOrElseUpdate" in {
      val cache = new SoftThreadLocal[String]()
      cache.getOrElseUpdate("value") mustEqual "value"

      var sideEffect = "1"
      cache.getOrElseUpdate({ sideEffect = "2"; "value2" }) mustEqual "value"
      sideEffect mustEqual "1"
    }

    "be thread safe" in {
      val cache = new SoftThreadLocal[String]()

      cache.put("v1")

      var second: String = null
      val t = new Thread(new Runnable() { override def run() = second = cache.getOrElseUpdate("v2") } )
      t.start()
      t.join()

      val first = cache.get

      first must beSome("v1")
      second mustEqual "v2"
    }

    "read expired references correctly" in {
      val cache = new SoftThreadLocal[String]()
      cache.put("v1")
      cache.get must beSome("v1")

      cache.cache.get().underlying.clear()
      cache.get must beNone
    }

    "update expired references correctly" in {
      val cache = new SoftThreadLocal[String]()
      cache.put("v1")
      cache.get must beSome("v1")

      cache.getOrElseUpdate("v2")
      cache.get must beSome("v1")

      cache.cache.get().underlying.clear()
      cache.get must beNone

      cache.getOrElseUpdate("v2")
      cache.get must beSome("v2")
    }
  }
}
