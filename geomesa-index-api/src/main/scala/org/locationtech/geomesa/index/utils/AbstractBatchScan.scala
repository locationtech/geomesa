/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.io.Closeable
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.annotation.tailrec

/**
  * Provides parallelism for scanning multiple ranges at a given time, for systems that don't
  * natively support that.
  *
  * Subclasses should generally only expose the `CloseableIterator` interface, and make sure to
  * invoke `start()` before returning to the caller.
  *
  * @param ranges ranges to scan
  * @param threads number of client threads to use for scanning
  * @param buffer max size of the buffer for storing results before they are read by the caller
  * @param sentinel singleton sentinel value used to indicate the completion of scanning threads
  * @tparam T range type
  * @tparam R scan result type
  */
abstract class AbstractBatchScan[T, R <: AnyRef](ranges: Seq[T], threads: Int, buffer: Int, sentinel: R)
    extends CloseableIterator[R] {

  import scala.collection.JavaConverters._

  require(threads > 0, "Thread count must be greater than 0")

  private val inQueue = new ConcurrentLinkedQueue(ranges.asJava)
  private val outQueue = new LinkedBlockingQueue[R](buffer)

  private val latch = new CountDownLatch(threads)
  private val terminator = new Terminator()
  private val pool = Executors.newFixedThreadPool(threads + 1)

  private var retrieved: R = _

  /**
    * Scan a single range, putting the results in the provided queue
    *
    * @param range range to scan
    * @param out results queue
    */
  protected def scan(range: T, out: BlockingQueue[R])

  /**
    * Start the threaded scans executing
    */
  protected def start(): CloseableIterator[R] = {
    var i = 0
    while (i < threads) {
      pool.submit(new SingleThreadScan())
      i += 1
    }
    pool.submit(terminator)
    pool.shutdown()
    this
  }

  override def hasNext: Boolean = {
    if (retrieved != null) {
      true
    } else {
      retrieved = outQueue.take
      if (!retrieved.eq(sentinel)) {
        true
      } else {
        outQueue.put(sentinel) // re-queue in case hasNext is called again
        retrieved = null.asInstanceOf[R]
        false
      }
    }
  }

  override def next(): R = {
    val n = retrieved
    retrieved = null.asInstanceOf[R]
    n
  }

  override def close(): Unit = {
    terminator.close()
    pool.shutdownNow()
  }

  /**
    * Exposed for testing only
    *
    * @param timeout timeout to wait
    * @return true if all threads have terminated, otherwise false
    */
  private [utils] def waitForDone(timeout: Long): Boolean = {
    val start = System.currentTimeMillis()
    while (true) {
      if (pool.isTerminated) {
        return true
      } else if (System.currentTimeMillis() - start > timeout) {
        return false
      } else {
        Thread.sleep(10)
      }
    }
    throw new IllegalStateException() // not possible to hit this, but the compiler can't figure that out
  }

  /**
    * Exposed for testing only
    *
    * @param timeout timeout to wait
    * @return true if full, otherwise false
    */
  private [utils] def waitForFull(timeout: Long): Boolean = {
    val start = System.currentTimeMillis()
    while (true) {
      if (outQueue.remainingCapacity() == 0) {
        return true
      } else if (System.currentTimeMillis() - start > timeout) {
        return false
      } else {
        Thread.sleep(10)
      }
    }
    throw new IllegalStateException() // not possible to hit this, but the compiler can't figure that out
  }

  /**
    * Pulls ranges off the queue and executes them
    */
  private class SingleThreadScan extends Runnable {
    override def run(): Unit = {
      try {
        var range = inQueue.poll()
        while (range != null && !Thread.currentThread().isInterrupted) {
          scan(range, outQueue)
          range = inQueue.poll()
        }
      } finally {
        latch.countDown()
      }
    }
  }

  /**
    * Injects the terminal value into the output buffer, once all the scans are complete
    */
  private class Terminator extends Runnable with Closeable {

    private val closed = new AtomicBoolean(false)

    override def run(): Unit = try { latch.await() } finally { terminate() }

    override def close(): Unit = closed.set(true)

    @tailrec
    private def terminate(): Unit = {
      // it's possible that the queue is full, in which case we can't immediately
      // add the sentinel to the queue to indicate to the client that scans are done
      if (closed.get) {
        // if the scan has been closed, then the client is done
        // reading and we don't mind dropping some results
        terminateWithDrops()
      } else {
        // otherwise we wait and give the client a chance to empty the queue
        val added = try { outQueue.offer(sentinel, 1000, TimeUnit.MILLISECONDS) } catch {
          case _: InterruptedException => terminateWithDrops(); true
        }
        if (!added) {
          terminate()
        }
      }
    }

    private def terminateWithDrops(): Unit = while (!outQueue.offer(sentinel)) { outQueue.poll() }
  }
}
