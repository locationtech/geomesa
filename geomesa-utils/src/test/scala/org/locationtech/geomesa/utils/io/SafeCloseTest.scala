/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.{Closeable, IOException}

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SafeCloseTest extends Specification with LazyLogging {

  class TestCloseable extends Closeable {
    var closed = false
    override def close(): Unit = closed = true
  }

  class RuntimeCloseable extends Closeable {
    throw new RuntimeException
    override def close(): Unit = {}
  }

  class IOCloseable extends Closeable {
    throw new IOException
    override def close(): Unit = {}
  }

  "WithClose" should {
    "close if there is an exception in block" in {
      val a = new TestCloseable
      val b = new TestCloseable
      val c = new TestCloseable

      WithClose[TestCloseable, TestCloseable, TestCloseable, Unit](a, b, c) {
        case (_, _, _) => throw new RuntimeException } must throwA[RuntimeException]

      a.closed must beTrue
      b.closed must beTrue
      c.closed must beTrue
    }

    "close if there is an exception initializing second instance" in {
      val a = new TestCloseable
      WithClose(a, new RuntimeCloseable) { case (_, _) => } must throwA[RuntimeException]
      a.closed must beTrue
    }

    "close if there is an exception initializing second and third instance" in {
      val a = new TestCloseable
      WithClose(a, new RuntimeCloseable, new IOCloseable) { case (_, _, _) => } must throwA[RuntimeException]
      a.closed must beTrue
    }

    "close if there is an exception initializing third instance" in {
      val a = new TestCloseable
      val b = new TestCloseable

      WithClose(a, b, new RuntimeCloseable) { case (_, _, _) => } must throwA[RuntimeException]

      a.closed must beTrue
      b.closed must beTrue
    }
  }
}


