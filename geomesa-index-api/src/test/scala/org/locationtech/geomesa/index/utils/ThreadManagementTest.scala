/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.junit.runner.RunWith
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThreadManagementTest extends Specification {

  "ManagedScan" should {
    "pre-close expired scans" in {
      val scanner = TestScanner(Seq("foo", "bar", "baz"))
      val scan = new ManagedScan(scanner, Timeout(0), "", None)
      scan.isTerminated must beTrue
      scan.hasNext must beTrue
      scan.next must throwA[RuntimeException]
    }
    "throw exception on next for expired scans" in {
      val scanner = TestScanner(Seq("foo", "bar", "baz"))
      val scan = new ManagedScan(scanner, Timeout("10 minutes"), "", None)
      scan.isTerminated must beFalse
      scan.hasNext must beTrue
      scan.next mustEqual "foo"
      scan.terminate()
      scanner.closed must beTrue
      scan.hasNext must beTrue
      scan.next must throwA[RuntimeException]
    }
    "throw exception on next for expired scans even if underlying iterator is empty" in {
      val scanner = TestScanner(Seq("foo"))
      val scan = new ManagedScan(scanner, Timeout("10 minutes"), "", None)
      scan.isTerminated must beFalse
      scan.hasNext must beTrue
      scan.next mustEqual "foo"
      scan.terminate()
      scanner.closed must beTrue
      scan.hasNext must beTrue
      scan.next must throwA[RuntimeException]
    }
    "throw exception on next for errors underlying iterator hasNext" in {
      val scanner = new TestScanner(Seq.empty) {
        override def iterator: Iterator[String] = new Iterator[String] {
          override def hasNext: Boolean = throw new RuntimeException("test")
          override def next(): String = null
        }
      }
      val scan = new ManagedScan(scanner, Timeout("10 minutes"), "", None)
      scan.isTerminated must beFalse
      scan.hasNext must beTrue
      scan.next must throwA[RuntimeException]
    }
  }

  case class TestScanner(values: Seq[String]) extends LowLevelScanner[String] {
    var closed = false
    override def iterator: Iterator[String] = values.iterator
    override def close(): Unit = closed = true
  }
}
