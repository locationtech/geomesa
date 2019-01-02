/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

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
