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
class SoftThreadLocalCacheTest extends Specification {

  "SoftThreadLocalCache" should {
    "implement map methods" in {
      new SoftThreadLocalCache[String, String]() must beAnInstanceOf[scala.collection.mutable.Map[String, String]]
    }

    "allow getOrElseUpdate" in {
      val cache = new SoftThreadLocalCache[String, String]()
      cache.getOrElseUpdate("k1", "v1") mustEqual "v1"
      var sideEffect = "1"
      cache.getOrElseUpdate("k1", { sideEffect = "2"; "v1" }) mustEqual "v1"
      sideEffect mustEqual "1"
    }

    "be thread safe" in {
      val obj1 = new AnyRef()
      val obj2 = new AnyRef()
      val cache = new SoftThreadLocalCache[String, AnyRef]()
      val first = cache.getOrElseUpdate("k1", obj1)
      var second: AnyRef = null
      val t = new Thread(new Runnable() { override def run = second = cache.getOrElseUpdate("k1", obj2) } )
      t.start()
      t.join()
      first must beTheSameAs(obj1)
      second must beTheSameAs(obj2)
      first must not beTheSameAs(second)
    }

    "read expired references correctly" in {
      val cache = new SoftThreadLocalCache[String, String]()
      cache.put("k1", "v1")
      cache.get("k1") must beSome("v1")
      cache.cache.get().get("k1").foreach(_.underlying.clear())
      cache.get("k1") must beNone
    }

    "update expired references correctly" in {
      val cache = new SoftThreadLocalCache[String, String]()
      cache.put("k1", "v1")
      cache.get("k1") must beSome("v1")
      cache.getOrElseUpdate("k1", "v2")
      cache.get("k1") must beSome("v1")
      cache.cache.get().get("k1").foreach(_.underlying.clear())
      cache.get("k1") must beNone
      cache.getOrElseUpdate("k1", "v2")
      cache.get("k1") must beSome("v2")
    }
  }
}
