/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class ByteArraysTest extends Specification with LazyLogging {

  "ByteArrays" should {
    "read and write ordered longs" in {
      val r = new Random(-6)
      val values = Seq(Long.MinValue, Long.MaxValue, 0L, -1L, 1L, -150L, 150L) ++ Seq.fill(10)(r.nextLong())
      foreach(values)(value => ByteArrays.readOrderedLong(ByteArrays.toOrderedBytes(value)) mustEqual value)
    }

    "read and write ordered shorts" in {
      val r = new Random(-6)
      val values = (Seq(Short.MinValue, Short.MaxValue, 0, -1, 1, -15, 15) ++ Seq.fill(10)(r.nextInt())).map(_.toShort)
      foreach(values)(value => ByteArrays.readOrderedShort(ByteArrays.toOrderedBytes(value)) mustEqual value)
    }
  }
}


