/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.apache.iceberg.DataFile
import org.apache.iceberg.expressions.Expressions
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergParquetScan
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.core.parquet.io.ParquetFileSystemWriter
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema
import org.locationtech.geomesa.utils.io.{CloseQuietly, WithClose}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/** Stages complete replacement files before publishing a single validated Iceberg rewrite. */
class FileSystemCompaction(storage: FileSystemStorage, partitions: Seq[Partition], target: Option[Long]) {

  import FileSystemCompaction.{Output, Plan}

  import scala.collection.JavaConverters._

  def compact(): Unit = {
    val plans = plan()
    val outputs = plans.map(prepare)
    commit(plans, outputs)
  }

  private[core] def plan(): Seq[Plan] = {
    storage.table.refresh()
    require(storage.schema.schema.sameSchema(storage.table.schema()), "Reload storage after a table schema change")
    require(storage.partitionSpecId == storage.table.spec().specId(), "Reload storage after a partition spec change")
    Option(storage.table.currentSnapshot()).toSeq.flatMap { snapshot =>
      val selected = partitions.toSet
      val files = WithClose(storage.table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) { tasks =>
        tasks.asScala.map(_.file().copy()).filter { file =>
          require(storage.table.spec().compatibleWith(storage.table.specs().get(file.specId())),
            "Compaction requires input files with a compatible partition spec")
          selected.contains(storage.metadata.partition(file)) && !target.exists(storage.sizer.fileIsSized(file, _))
        }.toList
      }
      files.groupBy(storage.metadata.partition).toSeq.map { case (partition, inputs) =>
        Plan(snapshot.snapshotId(), storage.table.schema().schemaId(), storage.table.spec().specId(), partition, inputs)
      }
    }
  }

  /** Writes all live rows without applying the caller's authorizations. No files are registered here. */
  private[core] def prepare(plan: Plan): Output = {
    validateSchema(plan.schemaId, plan.specId)
    val paths = ArrayBuffer.empty[String]
    val inputs = plan.files.map(_.location()).toSet
    val readSchema = storage.schema.read(None, None, Set.empty).schema
    val writeConf = storage.conf ++ ParquetFileSystemWriter.icebergBloomFilterConf(storage.table.properties()) ++
      Map(SimpleFeatureSchema.PartitionKey -> plan.partition.toString)
    val estimator = target.map(storage.sizer.estimator)
    var writer: ParquetFileSystemWriter = null
    var rows = 0L
    var totalBytes = 0L
    var remaining = estimator.fold(Long.MaxValue)(_.estimate(0L))

    def closeWriter(): Unit = if (writer != null) {
      writer.close()
      totalBytes += writer.size
      writer = null
    }

    try {
      WithClose(new IcebergParquetScan(storage.table, readSchema, Expressions.alwaysTrue(), 1,
          Some(inputs.contains), Some(plan.snapshotId))) { reader =>
        reader.foreach { feature =>
          if (writer == null) {
            val path = storage.newFilePath()
            paths += path
            writer = ParquetFileSystemWriter(storage.schema, writeConf, storage.table.io(), path, NoOpObserver)
          }
          writer.write(feature)
          rows += 1
          remaining -= 1
          if (remaining <= 0) {
            closeWriter()
            estimator.foreach(_.update(totalBytes, rows))
            remaining = estimator.fold(Long.MaxValue)(_.estimate(0L))
          }
        }
      }
      closeWriter()
      // Do not update table properties while files are merely staged.
      Output(plan.partition, paths.toList, rows)
    } catch {
      case NonFatal(e) =>
        CloseQuietly(Option(writer)).foreach(e.addSuppressed)
        paths.foreach { path =>
          try { storage.table.io().deleteFile(path) } catch { case NonFatal(cleanup) => e.addSuppressed(cleanup) }
        }
        throw e
    }
  }

  /**
   * The compaction reader applies positional deletes, including deletion vectors,
   * so replacement files contain only surviving rows.
   * Iceberg removes deletion-vector entries for rewritten inputs during commit.
   * Legacy positional-delete entries may remain until sequence-number cleanup removes them.
   * Physical files remain available to retained snapshots until snapshot expiration
   * can safely remove them.
   */
  private[core] def commit(plans: Seq[Plan], outputs: Seq[Output]): Unit = {
    if (plans.isEmpty) return
    require(plans.map(_.snapshotId).distinct.size == 1, "Compaction plans must share one snapshot")
    require(outputs.map(_.partition).toSet == plans.map(_.partition).toSet && outputs.size == plans.size,
      "Compaction results must cover every planned partition exactly once")
    require(plans.map(_.schemaId).distinct.size == 1 && plans.map(_.specId).distinct.size == 1,
      "Compaction plans must share one schema and partition spec")
    validateSchema(plans.head.schemaId, plans.head.specId)
    require(outputs.flatMap(_.paths).distinct.size == outputs.map(_.paths.size).sum,
      "Compaction results contain duplicate output paths")
    val spec = storage.table.specs().get(plans.head.specId)
    val replacements = outputs.flatMap { output =>
      val files = output.paths.map(storage.toDataFile(_, output.partition, spec))
      require(output.rows >= 0 && files.map(_.recordCount()).sum == output.rows,
        "Compaction result row counts do not match replacement files")
      files
    }
    validateSchema(plans.head.schemaId, plans.head.specId)
    val rewrite = storage.table.newRewrite().validateFromSnapshot(plans.head.snapshotId)
    plans.flatMap(_.files).foreach(rewrite.deleteFile)
    replacements.foreach(rewrite.addFile)
    // Iceberg validates both missing source files and concurrent row-level deletes during commit.
    // Leave staged files for orphan cleanup if commit fails: its final status can be unknown.
    rewrite.commit()
  }

  private def validateSchema(schemaId: Int, specId: Int): Unit = {
    storage.table.refresh()
    require(storage.table.schema().schemaId() == schemaId && storage.table.spec().specId() == specId,
      "Table schema or partition spec changed during compaction")
  }
}

private[core] object FileSystemCompaction {
  case class Plan(snapshotId: Long, schemaId: Int, specId: Int, partition: Partition, files: Seq[DataFile])
  case class Output(partition: Partition, paths: Seq[String], rows: Long)
}
