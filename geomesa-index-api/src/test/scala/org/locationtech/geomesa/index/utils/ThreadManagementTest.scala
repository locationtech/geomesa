/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.junit.runner.RunWith
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThreadManagementTest extends Specification {

  "ManagedScan" should {
    "pre-close expired scans" in {
      val scanner = TestScanner(Seq("foo", "bar", "baz"))
      val scan = new TestScan(Timeout(0), scanner)
      scan.isTerminated must beTrue
      scan.hasNext must beTrue
      scan.next must throwA[RuntimeException]
    }
    "throw exception on next for expired scans" in {
      val scanner = TestScanner(Seq("foo", "bar", "baz"))
      val scan = new TestScan(Timeout("10 minutes"), scanner)
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
      val scan = new TestScan(Timeout("10 minutes"), scanner)
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
      val scan = new TestScan(Timeout("10 minutes"), scanner)
      scan.isTerminated must beFalse
      scan.hasNext must beTrue
      scan.next must throwA[RuntimeException]
    }
  }

  class TestScan(override val timeout: Timeout, override protected val underlying: TestScanner)
      extends ManagedScan[String] {
    override protected def typeName: String = ""
    override protected def filter: Option[Filter] = None
  }

  case class TestScanner(values: Seq[String]) extends LowLevelScanner[String] {
    var closed = false
    override def iterator: Iterator[String] = values.iterator
    override def close(): Unit = closed = true
  }
}
