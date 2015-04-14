/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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
