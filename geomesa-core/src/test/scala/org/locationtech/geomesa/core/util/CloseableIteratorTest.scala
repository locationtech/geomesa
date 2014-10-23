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

package org.locationtech.geomesa.core.util

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CloseableIteratorTest extends Specification {

  "CloseableIterator" should {
    "provide an empty iterator" in {
      val ei = CloseableIterator.empty

      "that has no next element" >> {
        ei.hasNext shouldEqual false
      }

      "asking for an element should throw an exception" >> {
        {ei.next(); ()} should throwA[NoSuchElementException] // workaround for should not applying to Nothing
      }

      "closing it should succeed" >> {
        val ei = CloseableIterator.empty
        ei.close() should not(throwA[NullPointerException])
      }
    }

    "not smash the stack in ciFlatMap" >> {
      val f: Int => CloseableIterator[Int] = n =>
        if (n < 50000) CloseableIterator.empty
        else CloseableIterator(List(n).iterator)
      val ci = CloseableIterator((1 to 50000).iterator)
      ci.ciFlatMap(f).length should be equalTo 1
    }
  }
}
