/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.cassandra.utils

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, LinkedBlockingQueue}

import com.datastax.driver.core._
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConversions._

class BatchScan(session: Session, ranges: Seq[Statement], threads: Int, buffer: Int)
    extends CloseableIterator[Row] {

  import BatchScan.Sentinel

  require(threads > 0, "Thread count must be greater than 0")

  private val inQueue = new ConcurrentLinkedQueue(ranges)
  private val outQueue = new LinkedBlockingQueue[Row](buffer)

  private val pool = Executors.newFixedThreadPool(threads + 1)
  private val latch = new CountDownLatch(threads)

  (0 until threads).foreach(_ => pool.submit(new SingleThreadScan()))
  pool.submit(new Terminator)
  pool.shutdown()

  private var retrieved: Row = _

  override def hasNext: Boolean = {
    if (retrieved != null) {
      true
    } else {
      retrieved = outQueue.take
      if (!retrieved.eq(Sentinel)) {
        true
      } else {
        outQueue.put(Sentinel) // re-queue in case hasNext is called again
        retrieved = null
        false
      }
    }
  }

  override def next(): Row = {
    val n = retrieved
    retrieved = null
    n
  }

  override def close(): Unit = {
    pool.shutdownNow()
  }

  private class SingleThreadScan extends Runnable {
    override def run(): Unit = {
      try {
        var range = inQueue.poll
        while (range != null && !Thread.currentThread().isInterrupted) {
          session.execute(range).foreach(outQueue.put)
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
        outQueue.put(Sentinel)
      }
    }
  }
}

object BatchScan {
  private val Sentinel: Row = new AbstractGettableData(ProtocolVersion.NEWEST_SUPPORTED) with Row {
    override def getIndexOf(name: String): Int = -1
    override def getColumnDefinitions: ColumnDefinitions = null
    override def getToken(i: Int): Token = null
    override def getToken(name: String): Token = null
    override def getPartitionKeyToken: Token = null
    override def getType(i: Int): DataType = null
    override def getValue(i: Int): ByteBuffer = null
    override def getName(i: Int): String = null
    override def getCodecRegistry: CodecRegistry = null
  }
}
