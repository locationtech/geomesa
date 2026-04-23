/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.StorageFileObserver
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.{StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, FileSystemStorageFactory, Partition, PartitionKey, StorageMetadataCatalog}
import org.locationtech.geomesa.fs.storage.jobs.PartitionOutputFormat.SingleFileOutputFormat
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FileSizeEstimator}

import java.util.concurrent.CopyOnWriteArrayList

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

    private val fsc = {
      val hadoopConf = context.getConfiguration
      val conf = {
        val builder = Map.newBuilder[String, String]
        hadoopConf.forEach(e => builder += e.getKey -> e.getValue)
        builder.result()
      }
      val root = StorageConfiguration.getRootPath(hadoopConf)
      FileSystemContext.create(root, conf)
    }

    private val storage = {
      val hadoopConf = context.getConfiguration
      val encoding = StorageConfiguration.getEncoding(hadoopConf)
      val catalog = StorageMetadataCatalog(fsc)
      FileSystemStorageFactory(encoding).apply(fsc, catalog.load(StorageConfiguration.getSftName(hadoopConf)))
    }

    private val fileType = StorageConfiguration.getFileType(context.getConfiguration)
    private val fileSize = StorageConfiguration.getTargetFileSize(context.getConfiguration)

    private val counter = context.getCounter(Group, Features)
    private val cache = scala.collection.mutable.Map.empty[Set[PartitionKey], PartitionState]

    private val workPath = delegate.getOutputCommitter(context).asInstanceOf[FileOutputCommitter].getWorkPath

    private def newState(partitions: Set[PartitionKey]): PartitionState = {
      val estimator = fileSize.map(storage.sizer.estimator)
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

      import scala.collection.JavaConverters._

      private val files = new CopyOnWriteArrayList[StorageFile]()
      private val counter = context.getCounter(Group, StorageConfiguration.Counters.partition(partition.toString))

      private var observer: StorageFileObserver = _
      private var currentFile: String = _

      // noinspection AccessorLikeMethodIsEmptyParen
      def getFiles(): Seq[StorageFile] = files.asScala.toSeq

      def write(key: Void, value: SimpleFeature): Unit = observer.apply(value)

      def close(context: TaskAttemptContext): Unit = closeCurrentFile()

      protected def newWriter(): (Path, RecordWriter[Void, SimpleFeature]) = {
        closeCurrentFile()
        currentFile = FileSystemStorage.newFilePath(storage.metadata.sft.getTypeName, fileType, "parquet")
        observer = new StorageFileObserver(storage.metadata.sft)
        val path = new Path(workPath, currentFile)
        logger.debug(s"Creating record writer at path $path")
        // noinspection LanguageFeature
        (path, delegate.getRecordWriter(context, path))
      }

      private def closeCurrentFile(): Unit = {
        if (observer != null) {
          observer.close()
          val file = observer.file(currentFile, partition, StorageFileAction.Append)
          files.add(file)
          observer = null
          counter.increment(file.count)
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
          // TODO if we can access the underlying parquet writer we can check the size without closing the file
          writer.close(context)
          val length = storage.fs.size(path.toUri)
          logger.debug(s"File length: $length $path")
          writer = null
          // adjust our estimate to account for the actual bytes written
          total += length
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
