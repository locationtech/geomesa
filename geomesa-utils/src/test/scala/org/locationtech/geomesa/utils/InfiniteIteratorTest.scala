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
package org.locationtech.geomesa.utils

import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InfiniteIteratorTest extends Specification with Mockito {

  "stop after" should {

    import InfiniteIterator._

    "delegate until stop" >> {

      val end = mock[Iterator[String]]
      end.hasNext throws new IllegalArgumentException("You should have stopped.")
      end.next throws new IllegalArgumentException("You should have stopped.")

      val delegate = Iterator("A", "B") ++ end

      val result: Iterator[String] = delegate.stopAfter(_ == "B")

      result.hasNext must beTrue
      result.next() mustEqual "A"

      result.hasNext must beTrue
      result.next() mustEqual "B"

      result.hasNext must beFalse
      result.next() must throwA[NoSuchElementException]
    }

    "or the iterator is exhausted" >> {

      val delegate = Iterator("A", "B", "C")

      val result: Iterator[String] = delegate.stopAfter(_ == "D")

      result.hasNext must beTrue
      result.next() mustEqual "A"

      result.hasNext must beTrue
      result.next() mustEqual "B"

      result.hasNext must beTrue
      result.next() mustEqual "C"

      result.hasNext must beFalse
      result.next() must throwA[NoSuchElementException]
    }
  }
}
