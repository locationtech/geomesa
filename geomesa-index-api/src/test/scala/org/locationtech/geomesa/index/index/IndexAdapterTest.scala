/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexAdapterTest extends Specification {

  "IndexAdapter" should {
    "split empty ranges" in {
      forall(Seq(2, 4, 8, 16)) { count =>
        val splits = IndexAdapter.splitRange(Array.empty, Array.empty, count)
        splits must haveLength(count)
        splits.head._1 mustEqual Array.empty
        splits.last._2 mustEqual Array.empty
        forall(Seq(splits.head._2, splits.last._1) ++ splits.slice(1, splits.length - 1).flatMap { case (s, e) => Seq(s, e) })(_ must haveLength(1))
      }
    }
    "split prefix ranges" in {
      val start = Array[Byte](0)
      val stop = IndexAdapter.rowFollowingPrefix(start)

      forall(Seq(2, 4, 8, 16)) { count =>
        val splits = IndexAdapter.splitRange(start, stop, count)
        splits must haveLength(count)
        splits.head._1 mustEqual start
        splits.last._2 mustEqual stop
        forall(Seq(splits.head._2, splits.last._1) ++ splits.slice(1, splits.length - 1).flatMap { case (s, e) => Seq(s, e) }) { split =>
          split must haveLength(2)
          split.head mustEqual 0
        }
      }
    }
    "not split complex ranges" in {
      val start = Array[Byte](1, 2, 3)
      val stop = Array[Byte](4, 5, 6)
      val splits = IndexAdapter.splitRange(start, stop, 16)
      splits mustEqual Seq((start, stop))
    }
  }
}
