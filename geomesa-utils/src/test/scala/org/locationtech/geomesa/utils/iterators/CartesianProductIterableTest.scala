/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

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
