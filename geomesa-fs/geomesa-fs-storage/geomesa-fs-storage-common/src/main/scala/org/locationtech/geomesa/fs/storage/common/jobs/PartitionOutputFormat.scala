/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, PartitionKey, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, FileSystemStorageFactory}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.{FileTracker, UpdateObserver}
import org.locationtech.geomesa.fs.storage.common.SizeableFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.jobs.PartitionOutputFormat.SingleFileOutputFormat
import org.locationtech.geomesa.fs.storage.common.metadata.StorageMetadataCatalog
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FileSizeEstimator}

/**
  * Output format that writes to multiple partition files
  *
  * @param delegate underlying output format for a single file
  */
class PartitionOutputFormat(delegate: SingleFileOutputFormat) extends OutputFormat[Void, SimpleFeature] {

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, SimpleFeature] =
    new PartitionSchemeRecordWriter(context)

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter =
    delegate.getOutputCommitter(context)

  // same as FileOutputFormat, but doesn't require that output directory doesn't exist
  override def checkOutputSpecs(job: JobContext): Unit = {
    // Ensure that the output directory is set
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set")
    }
    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }

  private class PartitionSchemeRecordWriter(context: TaskAttemptContext)
      extends RecordWriter[Void, SimpleFeature] with LazyLogging {

    import StorageConfiguration.Counters.{Features, Group}

    private val storage = {
      val conf = context.getConfiguration
      val root = StorageConfiguration.getRootPath(conf)
      val fsc = FileSystemContext(root, conf)
      val encoding = StorageConfiguration.getEncoding(conf)
      val metadataType = StorageConfiguration.getMetadataType(conf)
      val metadataConfig = StorageConfiguration.getMetadataConfig(conf)
      val metadata = StorageMetadataCatalog(fsc, metadataType, metadataConfig).load(StorageConfiguration.getSftName(conf))
      FileSystemStorageFactory(fsc, metadata, encoding)
    }

    private val fileType = StorageConfiguration.getFileType(context.getConfiguration)
    private val fileSize = StorageConfiguration.getTargetFileSize(context.getConfiguration)

    private val counter = context.getCounter(Group, Features)
    private val cache = scala.collection.mutable.Map.empty[Set[PartitionKey], PartitionState]

    private val workPath = delegate.getOutputCommitter(context).asInstanceOf[FileOutputCommitter].getWorkPath

    private def newState(partitions: Set[PartitionKey]): PartitionState = {
      val estimator = storage match {
        case s: SizeableFileSystemStorage => s.targetSize(fileSize).map(s.estimator)
        case _ => None
      }
      estimator match {
        case None => new SinglePartitionState(Partition(partitions))
        case Some(e) => new ChunkedPartitionState(Partition(partitions), e, context)
      }
    }

    override def write(key: Void, value: SimpleFeature): Unit = {
      val partition = storage.metadata.schemes.map(_.getPartition(value))
      val state = cache.getOrElseUpdate(partition, newState(partition))
      state.write(key, value)
      counter.increment(1)
    }

    override def close(context: TaskAttemptContext): Unit = {
      cache.foreach { case (partition, state) =>
        logger.debug(s"Closing writer for $partition")
        state.close(context)
        state.getFiles().foreach(storage.metadata.addFile)
      }
      CloseWithLogging(storage)
    }

    private sealed abstract class PartitionState(partition: Partition) {

      private val fileTracker = new FileTracker(storage.metadata.sft, storage.metadata.schemes)
      private val counter = context.getCounter(Group, StorageConfiguration.Counters.partition(partition.toString))

      private var observer: UpdateObserver = _

      // noinspection AccessorLikeMethodIsEmptyParen
      def getFiles(): Seq[StorageFile] = fileTracker.getFiles()

      def write(key: Void, value: SimpleFeature): Unit = observer.apply(value)

      def close(context: TaskAttemptContext): Unit = closeCurrentFile()

      protected def newWriter(): (Path, RecordWriter[Void, SimpleFeature]) = {
        closeCurrentFile()
        val nextFile = StorageUtils.nextFile(storage.metadata.sft.getTypeName, fileType, "parquet")
        observer = new UpdateObserver(fileTracker, partition, nextFile, StorageFileAction.Append)
        val path = new Path(workPath, nextFile)
        logger.debug(s"Creating record writer at path $path")
        // noinspection LanguageFeature
        (path, delegate.getRecordWriter(context, path))
      }

      private def closeCurrentFile(): Unit = {
        if (observer != null) {
          observer.close()
          observer = null
          counter.increment(fileTracker.getFiles().last.count)
        }
      }
    }

    private class SinglePartitionState(partition: Partition) extends PartitionState(partition) {

      private val writer = newWriter()._2

      override def write(key: Void, value: SimpleFeature): Unit = {
        writer.write(key, value)
        super.write(key, value)
      }

      override def close(context: TaskAttemptContext): Unit = {
        writer.close(context)
        super.close(context)
      }
    }

    private class ChunkedPartitionState(partition: Partition, estimator: FileSizeEstimator, context: TaskAttemptContext)
        extends PartitionState(partition) {

      private var count = 0L // number of features written
      private var total = 0L // sum size of all finished chunks
      private var remaining = estimator.estimate(0L)

      logger.debug(s"Initial estimate: $remaining")

      private var path: Path = _
      private var writer: RecordWriter[Void, SimpleFeature] = _

      override def write(key: Void, value: SimpleFeature): Unit = {
        if (writer == null) {
          val (p, w) = newWriter()
          path = p
          writer = w
        }
        writer.write(key, value)
        count += 1
        remaining -= 1
        if (remaining == 0) {
          logger.debug(s"File length before closing: ${storage.context.fs.getFileStatus(path).getLen} $path")
          writer.close(context)
          logger.debug(s"File length after closing: ${storage.context.fs.getFileStatus(path).getLen} $path")
          writer = null
          // adjust our estimate to account for the actual bytes written
          total += storage.context.fs.getFileStatus(path).getLen
          estimator.update(total, count)
          remaining = estimator.estimate(0L)
          logger.debug(s"New estimate: $remaining")
        }
        super.write(key, value)
      }

      override def close(context: TaskAttemptContext): Unit = {
        if (writer != null) {
          writer.close(context)
        }
        super.close(context)
      }
    }
  }
}

object PartitionOutputFormat {
  type SingleFileOutputFormat = FileOutputFormat[Void, SimpleFeature] {
    def getRecordWriter(context: TaskAttemptContext, file: Path): RecordWriter[Void, SimpleFeature]
  }
}
