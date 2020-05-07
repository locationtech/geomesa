/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.utils.ThreadManagement.ManagedQuery
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThreadManagementTest extends Specification with LazyLogging {
  sequential

  val sorterTime  = 2000
  val scannerTime = 3000
  var startTime: Long = System.currentTimeMillis()

  "Slow code" should {
//    "be slow" >> {
//      startTime = System.currentTimeMillis()
//
//      val bs = getScans
//      val sorted = FirstClass(bs).toList
//
//      printTime("Total scan and sort")
//      sorted.size must equalTo(5)
//      ok
//    }

//    "be slow and run to completion with a long timeout" >> {
//      startTime = System.currentTimeMillis()
//
//      val bs = getScans
//      val sorted = new TimeoutWrapper(FirstClass(bs), 10000, "Long Timeout").toList
//
//      printTime("Total scan and sort")
//      sorted.size must equalTo(5)
//      ok
//    }

    "be slow and do something with a short timeout?!" >> {
      startTime = System.currentTimeMillis()

      val bs = getScans
      val sorted: Seq[String] = new TimeoutWrapper(SelfClosingIterator((0 until 1).toIterator).flatMap{i => FirstClass(bs).map(_.toInt).map(_.toString)},
        1000,
        "short timeout").toList

      printTime("Total scan and sort")
      sorted.size must equalTo(5)
      ok
    }
  }

  def printTime(msg: String, start: Long = startTime): Unit = {
    logger.warn(s"$msg took ${(System.currentTimeMillis()-start)/1000.0} seconds.")
  }

  def getScans(): CloseableIterator[Integer] = {
    ReportingClosureIterator(SlowBatchScanner((1 to 5).map(new Integer(_)), 8, 100))
  }

  case class FirstClass(scans: CloseableIterator[Integer]) extends CloseableIterator[Integer] {
    logger.warn(s"\tCreating BatchScanner and scanning")
    val closed = new AtomicBoolean(false)

    lazy val reduced = {
      val results = scans.toList
      printTime("\tGathering results took")
      //logger.warn(s"\tGotResults.  Calling to sorter at ${System.currentTimeMillis()}.")

      if (closed.get) {
        CloseableIterator.empty
      } else {
        val sortStart = System.currentTimeMillis()
        val sorter = new Sorter(results)
        val sorted: Seq[Integer] = sorter.doWork
        printTime("\tSorting", sortStart)

        CloseableIterator(sorted.toIterator, {})
      }
    }

    override def close(): Unit = {
      closed.set(true)
      scans.close()
      reduced.close()
    }

    override def hasNext: Boolean = reduced.hasNext

    override def next(): Integer = reduced.next
  }

  class TimeoutWrapper[T](ci: CloseableIterator[T], timeout: Long, testName: String) extends CloseableIterator[T] with ManagedQuery {
    val closed = new AtomicBoolean(false)
    val cancel = ThreadManagement.register(this)

    override def getTimeout: Long = timeout

    override def isClosed: Boolean = closed.get()

    override def debug: String = s"Time Wrapper for $testName!"

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        try {
          logger.warn(s"Trying to close the ci for Timeout Wrapper for $testName")
          ci.close()
        } finally {
          logger.warn(s"Canceling the QueryKiller for Timeout Wrapper for $testName")
          cancel.cancel(false)
        }
      }
    }

    override def hasNext: Boolean = ci.hasNext

    override def next(): T = ci.next

    override def interrupt: Unit = close()
  }

  case class ReportingClosureIterator[T](underlying: CloseableIterator[T]) extends CloseableIterator[T] {
    override def close(): Unit = {
      logger.warn("Calling close on underlying iterator!")
      underlying.close()
    }

    override def hasNext: Boolean = underlying.hasNext

    override def next(): T = underlying.next()
  }



//  def time[A](a: => A): (A, Long) = {
//    val now = System.currentTimeMillis()
//    logger.warn(s"Current time: $now.  Starting function\n")
//    val result = a
//    logger.warn(s"\nFinished function.  Elapsed time ${(System.currentTimeMillis() - now)/1000.0} seconds.")
//    (result, System.currentTimeMillis() - now)
//  }

  class SlowBatchScanner(ranges: Seq[Integer], threads: Int, buffer: Int)
    extends AbstractBatchScan[Integer, Integer](ranges, threads, buffer, Integer.MAX_VALUE) {
    override protected def scan(range: Integer, out: BlockingQueue[Integer]): Unit = {
      val scan = new SlowScanner(range)
      val result = scan.scan()
      out.put(result)
    }

    override def close(): Unit = {
      logger.warn("Closing BatchScanner!")
      super.close()
    }
  }

  object SlowBatchScanner {
    def apply(ranges: Seq[Integer], threads: Int, buffer: Int): CloseableIterator[Integer] =
      new SlowBatchScanner(ranges, threads, buffer).start()
  }

  class SlowScanner(i: Integer) {

    def scan(): Integer = {
      try {
        Thread.sleep(scannerTime)
      } catch {
        case e: InterruptedException =>
          logger.warn(s"Scan for $i was interrupted with InterruptedException")
          throw e
      }
      i
    }
  }

  class Sorter(input: Seq[Integer]) {
    var allocatedMemory: Int = input.map(_.toInt).sum

    def doWork: Seq[Integer] = {
      try {
        //logger.warn(s"\t\tSorter starting at ${System.currentTimeMillis()}")
        Thread.sleep(sorterTime)
        input.sorted
      } finally {
        //logger.warn(s"\t\tSorter finishing at ${System.currentTimeMillis()}")
        allocatedMemory = 0
      }
    }
  }

}


