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

package org.locationtech.geomesa.utils

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class CartesianProductIterableTest extends Specification {
  val LETTERS_UPPER = "ABCDEF".toSeq
  val NUMBERS = "012345".toSeq
  val LETTERS_LOWER = "xyz".toSeq

  "simple combinator" should {
    "yield expected size and results" in {
      val combinator = CartesianProductIterable(Seq(LETTERS_UPPER, NUMBERS,
                                                    LETTERS_LOWER))

      val itr = combinator.iterator
      itr.foldLeft(0)((i,combinationSeq) => {
        i match {
          case 0 => combinationSeq mustEqual Seq('A', '0', 'x')
          case 1 => combinationSeq mustEqual Seq('B', '0', 'x')
          case 42 => combinationSeq mustEqual Seq('A', '1', 'y')
          case 107 => combinationSeq mustEqual Seq('F', '5', 'z')
          case _ => // don't know; don't care
        }
        i + 1
      })

      combinator.expectedSize mustEqual 108
    }
  }
}
