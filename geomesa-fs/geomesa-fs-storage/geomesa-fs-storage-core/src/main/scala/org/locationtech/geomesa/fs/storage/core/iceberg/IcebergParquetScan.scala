/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg._
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.data.{InternalRecordWrapper, Record}
import org.apache.iceberg.expressions.{Evaluator, Expressions}
import org.apache.iceberg.parquet.Parquet
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Callable, Future}
import scala.collection.mutable.ArrayBuffer

class IcebergParquetScan(scan: TableScan, threads: Int) extends CloseableIterator[Record] {

  import scala.collection.JavaConverters._

  private val closed = new AtomicBoolean(false)
  private val ex = new CachedThreadPool(threads)
  private val futures = ArrayBuffer.empty[Future[CloseableIterator[Record]]]

//  private val tableSchema = scan.table().schema()
  private val projection = scan.schema()
  private val caseSensitive = scan.isCaseSensitive

  private val tasks = scan.planTasks()
  // TODO think we don't need to close this since we close the tasks?
  tasks.forEach(task => futures += ex.submit(new TaskRunnable(task)))

  private var current: CloseableIterator[Record] = CloseableIterator.empty[Record]

  override def hasNext: Boolean = {
    if (current.hasNext) {
      true
    } else {
      CloseWithLogging(current)
      current = null
      if (futures.isEmpty) {
        current = CloseableIterator.empty[Record]
        false
      } else {
        val i = futures.indexWhere(_.isDone)
        if (i == -1) {
          // TODO wait and find the first one that finishes
          current = futures.head.get()
        } else {
          current = futures.remove(i).get()
        }
        hasNext
      }
    }
  }

  override def next(): Record = current.next()

  override def close(): Unit = {
    closed.set(true)
    futures.foreach(_.cancel(true))
    if (current != null) {
      CloseWithLogging(current)
    }
    CloseWithLogging(tasks)
    // TODO close futures
    CloseWithLogging(ex)
  }

  private class TaskRunnable(task: CombinedScanTask) extends Callable[CloseableIterator[Record]] {
    override def call(): CloseableIterator[Record] = {
      CloseableIterator(task.files().iterator().asScala).flatMap { file =>
        if (file.deletes().isEmpty) {
          val inputFile = scan.table().io().newInputFile(file.file())
          val residual = file.residual()
          val read =
            Parquet.read(inputFile)
              .project(projection)
              .split(file.start(), file.length())
              .caseSensitive(caseSensitive)
              .reuseContainers() // TODO consider this
              .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(projection, fileSchema))
              .build[Record]()

          val iter = CloseableIterator(read.iterator())

          val filtered = if (residual == null || residual == Expressions.alwaysTrue()) { iter } else {
            val wrapper = new InternalRecordWrapper(projection.asStruct())
            val filter = new Evaluator(projection.asStruct(), residual, caseSensitive)
            iter.filter(r => filter.eval(wrapper.wrap(r)))
          }

          filtered
//    Map<Integer, ?> partition =
//        PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);
//
//    ReadBuilder<Record, ?> builder =
//        FormatModelRegistry.readBuilder(task.file().format(), Record.class, input);
//    if (reuseContainers) {
//      builder = builder.reuseContainers();
//    }
//
//    return builder
//        .project(fileProjection)
//        .idToConstant(partition)
//        .split(task.start(), task.length())
//        .caseSensitive(caseSensitive)
//        .filter(task.residual())
//        .build();
          // if (residual != null && residual != Expressions.alwaysTrue()) {
//      InternalRecordWrapper wrapper = new InternalRecordWrapper(recordSchema.asStruct());
//      Evaluator filter = new Evaluator(recordSchema.asStruct(), residual, caseSensitive);
//      return CloseableIterable.filter(records, record -> filter.eval(wrapper.wrap(record)));
//    }
        } else {
          ???
        }
      }
    }
  }
}

object IcebergParquetScan {

}