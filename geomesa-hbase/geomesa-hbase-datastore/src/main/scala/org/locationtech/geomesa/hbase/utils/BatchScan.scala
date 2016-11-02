/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.utils

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, LinkedBlockingQueue}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConversions._

class BatchScan(connection: Connection, tableName: TableName, ranges: Seq[Scan], threads: Int, buffer: Int)
    extends CloseableIterator[Result] {

  import BatchScan.Sentinel

  require(threads > 0, "Thread count must be greater than 0")


  private val table = connection.getTable(tableName)
  // TODO split up ranges if necessary so work is evenly distributed among threads
  private val inQueue = new ConcurrentLinkedQueue(ranges)
  private val outQueue = new LinkedBlockingQueue[Result](buffer)

  private val pool = Executors.newFixedThreadPool(threads + 1)
  private val latch = new CountDownLatch(threads)

  (0 until threads).foreach(_ => pool.submit(new SingleThreadScan()))
  pool.submit(new Terminator)
  pool.shutdown()

  private var retrieved = outQueue.take

  override def hasNext: Boolean = !retrieved.eq(Sentinel)

  override def next(): Result = {
    val n = retrieved
    retrieved = outQueue.take
    n
  }

  override def close(): Unit = {
    pool.shutdownNow()
    table.close()
  }

  private class SingleThreadScan extends Runnable {
    override def run(): Unit = {
      try {
        var range = inQueue.poll
        while (range != null && !Thread.currentThread().isInterrupted) {
          val scan = table.getScanner(range)
          try {
            scan.iterator.foreach(outQueue.put)
          } finally {
            scan.close()
          }
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
  private val Sentinel = new Result
}
