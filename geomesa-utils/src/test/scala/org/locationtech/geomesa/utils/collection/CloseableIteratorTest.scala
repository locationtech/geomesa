/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.collection

import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CloseableIteratorTest extends Specification with Mockito {

  "CloseableIterator" should {
    "close" >> {
      var closed = false
      val iter = CloseableIterator(Iterator.empty, { closed = true })
      closed must beFalse
      iter.close()
      closed must beTrue
    }
    "close with flatmap" >> {
      var closed0, closed1, closed2 = false
      val result = CloseableIterator(Iterator(0, 1), { closed0 = true }).ciFlatMap { i =>
        if (i == 0) {
          CloseableIterator(Iterator(2, 3), { closed1 = true })
        } else {
          CloseableIterator(Iterator(4, 5), { closed2 = true })
        }
      }
      foreach(Seq(closed0, closed1, closed2))(_ must beFalse)
      result.toSeq mustEqual Seq(2, 3, 4, 5)
      result.close()
      foreach(Seq(closed0, closed1, closed2))(_ must beTrue)
    }
    "close with concatenate" >> {
      var closed0, closed1, closed2 = false
      val result = CloseableIterator(Iterator(0, 1), { closed0 = true }) ++
          CloseableIterator(Iterator(2, 3), { closed1 = true }) ++
          CloseableIterator(Iterator(4, 5), { closed2 = true })
      result.toSeq mustEqual Seq(0, 1, 2, 3, 4, 5)
      result.close()
      foreach(Seq(closed0, closed1, closed2))(_ must beTrue)
    }
  }
}
