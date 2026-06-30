/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg._
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.parquet.Parquet
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

class IcebergParquetScan(scan: TableScan, threads: Int) extends CloseableIterator[Record] with LazyLogging {

  import scala.collection.JavaConverters._

  private val sharedQueue = new LinkedBlockingQueue[Record](2000000)
  private val localQueue = new java.util.LinkedList[Record]()

//  private val tableSchema = scan.table().schema()
  private val projection = scan.schema()
  private val caseSensitive = scan.isCaseSensitive

  private val closed = new AtomicBoolean(false)

  private val ex = new CachedThreadPool(threads)
  private val tasks = scan.planTasks()

  var i = 0
  logger.debug("Submitting tasks")
  tasks.forEach { task =>
    logger.trace(s"Submitting task: $task")
    ex.submit(new TaskRunnable(task))
    i += 1
  }
  logger.debug(s"Submitted $i tasks, using $threads threads")
  ex.shutdown()

  private var current: Record = _

  override def hasNext: Boolean = {
    if (current != null) {
      return true
    }
    current = localQueue.pollFirst()
    if (current != null) {
      return true
    }

    while (!ex.isTerminated) {
      current = sharedQueue.poll(100, TimeUnit.MILLISECONDS)
      if (current != null) {
        sharedQueue.drainTo(localQueue, 10000)
        return true
      }
    }

    // last check - if ex.isTerminated, the queue should have whatever values are left
    current = sharedQueue.poll()
    if (current != null) {
      sharedQueue.drainTo(localQueue, 10000)
      true
    } else {
      false
    }
  }

  override def next(): Record = {
    if (hasNext) {
      val ret = current
      current = null
      ret
    } else {
      Iterator.empty.next
    }
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try {
        ex.shutdownNow()
        ex.awaitTermination(2, TimeUnit.SECONDS)
      } finally {
        CloseWithLogging(Seq(tasks) ++ Option(scan).collect { case c: Closeable => c })
      }
    }
  }

  private def readFile(task: FileScanTask): CloseableIterable[Record] = {
    val inputFile = scan.table().io().newInputFile(task.file())
    logger.debug(s"Reading file ${inputFile.location()} [${task.start()}:${task.length()}]")
    Parquet.read(inputFile)
      .project(projection)
      .split(task.start(), task.length())
      .caseSensitive(caseSensitive)
      .filter(task.residual())
      // TODO implement ParquetValueReader directly instead of using records
      .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(projection, fileSchema))
      .build[Record]()
  }

  private class TaskRunnable(task: CombinedScanTask) extends Runnable {
    override def run(): Unit = {
      task.files().iterator().asScala.foreach { file =>
        if (!closed.get()) {
          if (file.deletes().isEmpty) {
            WithClose(readFile(file)) { read =>
              WithClose(read.iterator()) { iter =>
                iter.asScala.foreach(sharedQueue.put)
              }
            }
          } else {
            // TODO implement deletes
            ???
          }
        }
      }
    }
  }
}
