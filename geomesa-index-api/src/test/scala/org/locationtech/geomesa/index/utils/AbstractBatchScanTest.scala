/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AbstractBatchScanTest extends Specification {

  class TestBatchScan(ranges: Seq[String], threads: Int, buffer: Int)
      extends AbstractBatchScan[String, String](ranges, threads, buffer, "SENTINEL") {
    override protected def scan(range: String): CloseableIterator[String] =
      CloseableIterator(range.iterator.map(_.toString))
  }

  object TestBatchScan {
    def apply(ranges: Seq[String], threads: Int, buffer: Int): TestBatchScan =
      new TestBatchScan(ranges, threads, buffer).start().asInstanceOf[TestBatchScan]
  }

  class ErrorScan(ranges: Seq[String], err: String) extends TestBatchScan(ranges, 2, 100) {
    override protected def scan(range: String): CloseableIterator[String] =
      if (range == err) { throw new RuntimeException(err) } else { super.scan(range) }
  }

  object ErrorScan {
    def apply(ranges: Seq[String], err: String): CloseableIterator[String] = new ErrorScan(ranges, err).start()
  }

  "AbstractBatchScan" should {
    "scan with multiple threads" in {
      val iter = TestBatchScan(Seq("foo", "bar"), 2, 100)
      iter.waitForDone(1000) must beTrue
      SelfClosingIterator(iter).toList must containTheSameElementsAs(Seq("f", "o", "o", "b", "a", "r"))
    }
    "scan exceeding the buffer size" in {
      val iter = TestBatchScan(Seq("foo", "bar"), 2, 2)
      iter.waitForFull(1000) must beTrue
      SelfClosingIterator(iter).toList must containTheSameElementsAs(Seq("f", "o", "o", "b", "a", "r"))
      iter.waitForDone(1000) must beTrue
    }
    "handle being closed prematurely" in {
      val iter = TestBatchScan(Seq("foo", "bar"), 2, 100)
      iter.close()
      iter.waitForDone(1000) must beTrue
      iter.toList must not(throwAn[Exception])
    }
    "handle being closed prematurely with a full buffer" in {
      val iter = TestBatchScan(Seq("foo", "bar"), 2, 2)
      iter.waitForFull(1000) must beTrue
      iter.close()
      iter.waitForDone(1000) must beTrue
      // verify that the terminator dropped a result to set the terminal value
      iter.toList must haveLength(1)
    }
    "re-throw exceptions from scanning" in {
      val ranges = Seq("foo", "bar", "baz")
      foreach(ranges) { r =>
        val iter = ErrorScan(ranges, r)
        try {
          iter.toList must throwA[RuntimeException](r)
        } finally {
          iter.close()
        }
      }
    }
  }
}
