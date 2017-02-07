/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.CloseableIterator

import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CloseableIteratorTest extends org.specs2.mutable.Spec {

  "CloseableIterator" should {
    "provide an empty iterator" in {
      val ei = CloseableIterator.empty

      "that has no next element" >> {
        ei.hasNext mustEqual false
      }

      "asking for an element should throw an exception" >> {
        {ei.next(); ()} must throwA[NoSuchElementException] // workaround for should not applying to Nothing
      }

      "closing it should succeed" >> {
        val ei = CloseableIterator.empty
        ei.close() must not(throwA[NullPointerException])
      }

      ok
    }

    "not smash the stack in ciFlatMap" >> {
      val f: Int => CloseableIterator[Int] = n =>
        if (n < 50000) CloseableIterator.empty
        else CloseableIterator(List(n).iterator)
      val ci = CloseableIterator((1 to 50000).iterator)
      ci.ciFlatMap(f).length mustEqual 1
    }
  }
}
