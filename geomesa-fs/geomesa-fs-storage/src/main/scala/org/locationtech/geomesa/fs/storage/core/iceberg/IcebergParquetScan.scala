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
import org.apache.iceberg.expressions.{Evaluator, Expression, Expressions}
import org.apache.iceberg.io.DeleteSchemaUtil
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.TypeUtil
import org.geotools.api.feature.simple.SimpleFeature
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
 * @param table table to scan
 * @param schema read schema
 * @param filter filter
 * @param threads number of threads used to execute
 * @param fileFilter optional filter for restricting the files that are scanned
 */
class IcebergParquetScan(
    table: Table,
    schema: SimpleFeatureIcebergSchema,
    filter: Expression,
    threads: Int,
    fileFilter: Option[String => Boolean] = None
  ) extends CloseableIterator[SimpleFeature] with LazyLogging {

  import scala.collection.JavaConverters._

  private val queue = new LinkedBlockingQueue[StructLike](10000)
  private val closed = new AtomicBoolean(false)

  private lazy val deleteProjection =
    if (schema.schema.findField(MetadataColumns.ROW_POSITION.fieldId()) != null) { schema.schema } else {
      TypeUtil.join(schema.schema, new Schema(MetadataColumns.ROW_POSITION))
    }

  private val row = StructSimpleFeature(schema)

  // WithClose here shuts down the executor service, but already submitted tasks will still be executed
  private val ex =
    WithClose(new CachedThreadPool(threads)) { pool =>
      // note: exclude z2 cols even if there's no transform
      val scan = table.newScan().caseSensitive(false).project(schema.schema).filter(filter)
      try {
        WithClose(scan.planTasks()) { plan =>
          WithClose(plan.iterator()) { tasks =>
            var taskCount = 0
            var fileCount = 0
            logger.debug("Submitting tasks")
            tasks.forEachRemaining { task =>
              logger.trace(s"Submitting task with ${task.filesCount()} file(s)")
              pool.submit(new TaskRunnable(task))
              taskCount += 1
              fileCount += task.filesCount()
            }
            logger.debug(s"Submitted $taskCount tasks (scanning $fileCount total files) using $threads threads")
          }
        }
      } finally {
        CloseWithLogging(Option(scan).collect { case c: Closeable => c })
      }
      pool
    }

  private var current: StructLike = _

  override def hasNext: Boolean = {
    if (current != null) {
      return true
    }
    current = queue.poll()
    if (current != null) {
      return true
    }

    if (closed.get()) {
      false
    } else if (ex.isTerminated) {
      current = queue.poll()
      current != null
    } else {
      while (current == null && !ex.isTerminated) {
        current = queue.poll(100, TimeUnit.MILLISECONDS)
      }
      current != null
    }
  }

  override def next(): SimpleFeature = {
    if (hasNext) {
      row.setRow(current)
      current = null
      row
    } else {
      Iterator.empty.next
    }
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      ex.shutdownNow()
      ex.awaitTermination(1, TimeUnit.SECONDS)
    }
  }

  private def readFile(task: FileScanTask, projection: Schema): CloseableIterator[StructLike] = {
    val inputFile = table.io().newInputFile(task.file())
    if (fileFilter.exists(_.apply(inputFile.location()) == false)) {
      logger.debug(s"Skipping file ${inputFile.location()} [${task.start()}:${task.length()}] due to file filter")
      CloseableIterator.empty[StructLike]
    } else {
      logger.debug(s"Reading file ${inputFile.location()} [${task.start()}:${task.length()}] with filter: ${task.residual()}")
      // we have to pass in the file path as a constant
      val idToConstant = java.util.Map.of[Integer, Any](MetadataColumns.FILE_PATH_COLUMN_ID, inputFile.location())
      // note: can't reuse containers b/c we put everything onto the queue - reading row-by-row with reused containers is much slower
      val reader =
        Parquet.read(inputFile)
          .project(projection)
          .split(task.start(), task.length())
          .caseSensitive(false)
          .filter(task.residual())
          .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(projection, fileSchema, idToConstant))
          .build[Record]()
      val iter = reader.iterator()
      CloseableIterator(iter.asScala, CloseWithLogging(Seq(iter, reader)))
    }
  }

  private def readDeleteFile(delete: DeleteFile, dataFilePath: String): PositionDeleteIndex = {
    require(delete.content() == FileContent.POSITION_DELETES,
      s"Only positional deletes are supported, but got: ${delete.content()}")
    logger.debug(s"Reading delete file ${delete.location()} [${delete.contentSizeInBytes()}]")
    val deleteFileSchema = DeleteSchemaUtil.pathPosSchema()
    val builder =
      Parquet.read(table.io().newInputFile(delete))
        .project(deleteFileSchema)
        .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(deleteFileSchema, fileSchema))
        .filter(Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), dataFilePath))
    WithClose(builder.build()) { deletes =>
      Deletes.toPositionIndex(dataFilePath, deletes, delete)
    }
  }

  private class TaskRunnable(val task: CombinedScanTask) extends Runnable {
    override def run(): Unit = {
      try {
        task.files().iterator().asScala.foreach { file =>
          if (!closed.get()) {
            val deleteIndex = if (file.deletes().isEmpty) { None } else {
              val deletes = file.deletes().asScala.map(readDeleteFile(_, file.file().location()))
              Some(PositionDeleteIndexUtil.merge(deletes.asJava))
            }
            val projection = if (deleteIndex.isEmpty) { schema.schema } else { deleteProjection }

            WithClose(readFile(file, projection)) { iter =>
              val withDeletes = deleteIndex.fold(iter) { index =>
                val position = projection.accessorForField(MetadataColumns.ROW_POSITION.fieldId())
                iter.filterNot(r => index.isDeleted(position.get(r).asInstanceOf[java.lang.Long]))
              }
              val residual = file.residual()
              val filtered = if (residual == null || residual == Expressions.alwaysTrue()) { withDeletes } else {
                val filter = new Evaluator(projection.asStruct(), residual, false)
                val wrapper = new InternalRecordWrapper(projection.asStruct())
                withDeletes.filter(r => filter.eval(wrapper.wrap(r)))
              }
              filtered.foreach(queue.put)
            }
          }
        }
      } catch {
        case NonFatal(e) => logger.error("Error running scan task:", e)
      }
    }
  }
}
