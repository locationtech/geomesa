/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.util.Map.Entry
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, Future, LinkedBlockingQueue, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.ScannerBase
import org.apache.accumulo.core.data.{Key, Value}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloQueryPlan.JoinFunction
import org.locationtech.geomesa.accumulo.index.BatchScanPlan

import scala.collection.JavaConversions._

class BatchMultiScanner(ds: AccumuloDataStore,
                        in: ScannerBase,
                        join: BatchScanPlan,
                        joinFunction: JoinFunction,
                        numThreads: Int = 12,
                        batchSize: Int = 32768)
  extends Iterable[java.util.Map.Entry[Key, Value]] with AutoCloseable with LazyLogging {

  require(batchSize > 0, f"Illegal batchSize ($batchSize%d). Value must be > 0")
  require(numThreads > 0, f"Illegal numThreads ($numThreads%d). Value must be > 0")
  logger.trace(f"Creating BatchMultiScanner with batchSize $batchSize%d and numThreads $numThreads%d")

  // calculate authorizations up front so that our multi-threading doesn't mess up auth providers
  private val auths = Option(ds.auths)
  private val executor = Executors.newFixedThreadPool(numThreads)

  private val inQ  = new LinkedBlockingQueue[Entry[Key, Value]](batchSize)
  private val outQ = new LinkedBlockingQueue[Entry[Key, Value]](batchSize)

  private val inDone  = new AtomicBoolean(false)
  private val outDone = new AtomicBoolean(false)

  executor.submit(new Runnable {
    override def run(): Unit = {
      try {
        in.iterator().foreach(inQ.put)
      } finally {
        inDone.set(true)
      }
    }
  })

  executor.submit(new Runnable {
    override def run(): Unit = {
      try {
        val tasks = collection.mutable.ListBuffer.empty[Future[_]]
        while (!inDone.get || inQ.size() > 0) {
          val entry = inQ.poll(5, TimeUnit.MILLISECONDS)
          if (entry != null) {
            val entries = collection.mutable.ListBuffer(entry)
            inQ.drainTo(entries)
            val task = executor.submit(new Runnable {
              override def run(): Unit = {
                val iterator = join.copy(ranges = entries.map(joinFunction)).scanEntries(ds, auths)
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

  override def close() {
    if (!executor.isShutdown) {
      executor.shutdownNow()
    }
    in.close()
  }

  override def iterator: Iterator[Entry[Key, Value]] = new Iterator[Entry[Key, Value]] {

    private var prefetch: Entry[Key, Value] = null

    private def prefetchIfNull() = {
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
  }
}
