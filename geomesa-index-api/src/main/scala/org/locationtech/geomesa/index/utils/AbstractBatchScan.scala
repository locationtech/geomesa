/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.concurrent._

import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConversions._

abstract class AbstractBatchScan[T, R <: AnyRef](ranges: Seq[T], threads: Int, buffer: Int)
    extends CloseableIterator[R] {

  require(threads > 0, "Thread count must be greater than 0")

  private val inQueue = new ConcurrentLinkedQueue(ranges)
  private val outQueue = new LinkedBlockingQueue[R](buffer)

  private val pool = Executors.newFixedThreadPool(threads + 1)
  private val latch = new CountDownLatch(threads)

  private val sentinel: R = singletonSentinel
  private var retrieved: R = _

  (0 until threads).foreach(_ => pool.submit(new SingleThreadScan()))
  pool.submit(new Terminator)
  pool.shutdown()

  protected def scan(range: T, out: BlockingQueue[R])
  protected def singletonSentinel: R

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

  override def close(): Unit = pool.shutdownNow()

  private class SingleThreadScan extends Runnable {
    override def run(): Unit = {
      try {
        var range = inQueue.poll
        while (range != null && !Thread.currentThread().isInterrupted) {
          scan(range, outQueue)
          range = inQueue.poll
        }
      } finally {
        latch.countDown()
      }
    }
  }

  private class Terminator extends Runnable {
    override def run(): Unit = {
      try {
        latch.await()
      } finally {
        outQueue.put(sentinel)
      }
    }
  }
}
