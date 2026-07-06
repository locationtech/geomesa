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
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.data.{InternalRecordWrapper, Record}
import org.apache.iceberg.deletes.{Deletes, PositionDeleteIndex, PositionDeleteIndexUtil}
import org.apache.iceberg.expressions.{Evaluator, Expressions}
import org.apache.iceberg.io.DeleteSchemaUtil
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.TypeUtil
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.util.control.NonFatal

/**
 * Reads parquet files based on an iceberg table scan
 *
 * @param scan scan
 * @param threads number of threads used to execute
 * @param fileFilter optional filter for restricting the files that are scanned
 */
class IcebergParquetScan(scan: TableScan, threads: Int, fileFilter: Option[String => Boolean] = None)
    extends CloseableIterator[Record] with LazyLogging {

  import scala.collection.JavaConverters._

  private val sharedQueue = new LinkedBlockingQueue[Record](2000000)
  private val localQueue = new java.util.LinkedList[Record]()

  private val projection = scan.schema()
  private lazy val deleteProjection =
    if (projection.findField(MetadataColumns.ROW_POSITION.fieldId()) != null) { projection } else {
      TypeUtil.join(projection, new Schema(MetadataColumns.ROW_POSITION))
    }

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

  private def readFile(task: FileScanTask, projection: Schema): CloseableIterator[Record] = {
    val inputFile = scan.table().io().newInputFile(task.file())
    if (fileFilter.exists(_.apply(inputFile.location()) == false)) {
      logger.debug(s"Skipping file ${inputFile.location()} [${task.start()}:${task.length()}] due to file filter")
      CloseableIterator.empty[Record]
    } else {
      logger.debug(s"Reading file ${inputFile.location()} [${task.start()}:${task.length()}]")
      // we have to pass in the file path as a constant
      val idToConstant = java.util.Map.of[Integer, Any](MetadataColumns.FILE_PATH_COLUMN_ID, inputFile.location())
      val reader =
        Parquet.read(inputFile)
          .project(projection)
          .split(task.start(), task.length())
          .caseSensitive(caseSensitive)
          .filter(task.residual())
          // TODO implement ParquetValueReader directly instead of using records
          .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(projection, fileSchema, idToConstant))
          .build[Record]()
      val iter = reader.iterator()
      CloseableIterator(iter.asScala, CloseWithLogging(Seq(iter, reader)))
    }
  }

  private def readDeleteFile(delete: DeleteFile, dataFilePath: String): PositionDeleteIndex = {
    require(delete.content() == FileContent.POSITION_DELETES,
      s"Only positional deletes are supported, but got: ${delete.content()}")
    val deleteFileSchema = DeleteSchemaUtil.pathPosSchema()
    val builder =
      Parquet.read(scan.table().io().newInputFile(delete))
        .project(deleteFileSchema)
        .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(deleteFileSchema, fileSchema))
        .filter(Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), dataFilePath))
    WithClose(builder.build()) { deletes =>
      Deletes.toPositionIndex(dataFilePath, deletes, delete)
    }
  }

  private class TaskRunnable(task: CombinedScanTask) extends Runnable {
    override def run(): Unit = {
      try {
        task.files().iterator().asScala.foreach { file =>
          if (!closed.get()) {
            val deleteIndex = if (file.deletes().isEmpty) { None } else {
              val deletes = file.deletes().asScala.map(readDeleteFile(_, file.file().location()))
              Some(PositionDeleteIndexUtil.merge(deletes.asJava))
            }
            val projection = if (deleteIndex.isEmpty) { IcebergParquetScan.this.projection } else { deleteProjection }

            WithClose(readFile(file, projection)) { iter =>
              lazy val wrapper = new InternalRecordWrapper(projection.asStruct())
              val withDeletes = deleteIndex.fold(iter) { index =>
                val position = projection.accessorForField(MetadataColumns.ROW_POSITION.fieldId())
                iter.filterNot(r => index.isDeleted(position.get(wrapper.wrap(r)).asInstanceOf[java.lang.Long]))
              }
              val residual = file.residual()
              val filtered = if (residual == null || residual == Expressions.alwaysTrue()) { withDeletes } else {
                val filter = new Evaluator(projection.asStruct(), residual, caseSensitive)
                withDeletes.filter(r => filter.eval(wrapper.wrap(r)))
              }
              filtered.foreach(sharedQueue.put)
            }
          }
        }
      } catch {
        case NonFatal(e) => logger.error("Error running scan task:", e)
      }
    }
  }
}
