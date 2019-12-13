/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SortedMergeIteratorTest extends Specification with Mockito {

  "SortedMergeIterator" should {
    "merge multiple sorted streams" in {
      val s1 = CloseableIterator(Iterator(1, 7, 8))
      val s2 = CloseableIterator(Iterator(2, 3, 9))
      val s3 = CloseableIterator(Iterator(5, 10))

      val iter = new SortedMergeIterator(Seq(s1, s2, s3))

      iter.toList mustEqual Seq(1, 2, 3, 5, 7, 8, 9, 10)
    }
  }
}
