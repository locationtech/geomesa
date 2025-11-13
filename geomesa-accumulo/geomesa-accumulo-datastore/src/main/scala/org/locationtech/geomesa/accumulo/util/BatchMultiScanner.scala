/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.ScannerBase
import org.apache.accumulo.core.data.{Key, Value}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.util.Map.Entry
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, Future, LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable.ListBuffer

/**
 * Runs a join scan against two tables
 *
 * @param in input scan
 * @param join join scan
 * @param numThreads threads
 * @param batchSize batch size
 */
class BatchMultiScanner(
    in: ScannerBase,
    join: Seq[Entry[Key, Value]] => CloseableIterator[Entry[Key, Value]],
    numThreads: Int = 12,
    batchSize: Int = 32768
  ) extends CloseableIterator[java.util.Map.Entry[Key, Value]] with LazyLogging {

  import scala.collection.JavaConverters._

  require(batchSize > 0, f"Illegal batchSize ($batchSize%d). Value must be > 0")
  require(numThreads > 0, f"Illegal numThreads ($numThreads%d). Value must be > 0")
  logger.trace(f"Creating BatchMultiScanner with batchSize $batchSize%d and numThreads $numThreads%d")

  private val executor = Executors.newFixedThreadPool(numThreads)

  private val inQ  = new LinkedBlockingQueue[Entry[Key, Value]](batchSize)
  private val outQ = new LinkedBlockingQueue[Entry[Key, Value]](batchSize)

  private val inDone  = new AtomicBoolean(false)
  private val outDone = new AtomicBoolean(false)

  private var prefetch: Entry[Key, Value] = _

  private def prefetchIfNull(): Unit = {
    // loop while we might have another and we haven't set prefetch
    while (prefetch == null && (!outDone.get || outQ.size > 0)) {
      prefetch = outQ.poll(5, TimeUnit.MILLISECONDS)
    }
  }

  // must attempt a prefetch since we don't know whether or not the outQ
  // will actually be filled with an item (filters may not match and the
  // in scanner may never return a range)
  override def hasNext(): Boolean = {
    prefetchIfNull()
    prefetch != null
  }

  override def next(): Entry[Key, Value] = {
    prefetchIfNull()
    val ret = prefetch
    prefetch = null
    ret
  }

  executor.submit(new Runnable {
    override def run(): Unit = {
      try {
        in.asScala.foreach(inQ.put)
      } finally {
        inDone.set(true)
      }
    }
  })

  executor.submit(new Runnable {
    override def run(): Unit = {
      try {
        val tasks = ListBuffer.empty[Future[_]]
        while (!inDone.get || inQ.size() > 0) {
          val entry = inQ.poll(5, TimeUnit.MILLISECONDS)
          if (entry != null) {
            val entries = ListBuffer(entry)
            inQ.drainTo(entries.asJava)
            val task = executor.submit(new Runnable {
              override def run(): Unit = {
                val iterator = join(entries.toSeq)
                try {
                  iterator.foreach(outQ.put)
                } finally {
                  iterator.close()
                }
              }
            })
            tasks.append(task)
          }
        }
        tasks.foreach(_.get)
      } catch {
        case _: InterruptedException =>
      } finally {
        executor.shutdown()
        outDone.set(true)
      }
    }
  })

  override def close(): Unit = {
    if (!executor.isShutdown) {
      executor.shutdownNow()
    }
    in.close()
  }
}
