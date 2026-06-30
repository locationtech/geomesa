/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.jobs.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, Partition, PartitionKey, StorageCatalog}
import org.locationtech.geomesa.fs.storage.jobs.StorageConfiguration
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FileSizeEstimator}

import java.net.URI
import java.util.concurrent.CopyOnWriteArrayList

/**
  * Output format that writes to multiple partition files
  */
class ParquetPartitionOutputFormat extends OutputFormat[Void, SimpleFeature] {

  private val delegate = new ParquetSimpleFeatureOutputFormat()

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
        hadoopConf.forEach(e => builder += e.getKey -> hadoopConf.get(e.getKey)) // use .get to resolve envs
        builder.result()
      }
      val root = StorageConfiguration.getRootPath(hadoopConf)
      FileSystemContext.create(root, conf)
    }

    private val catalog = StorageCatalog(fsc)
    private val storage = catalog.load(StorageConfiguration.getSftName(context.getConfiguration))

    private val fileSize = StorageConfiguration.getTargetFileSize(context.getConfiguration)

    private val counter = context.getCounter(Group, Features)
    private val cache = scala.collection.mutable.Map.empty[Seq[PartitionKey], PartitionState]

    private val workPath = delegate.getOutputCommitter(context).asInstanceOf[FileOutputCommitter].getWorkPath

    private def newState(partitions: Seq[PartitionKey]): PartitionState = {
      val estimator = fileSize.map(storage.sizer.estimator)
      estimator match {
        case None => new SinglePartitionState(Partition(partitions))
        case Some(e) => new ChunkedPartitionState(Partition(partitions), e, context)
      }
    }

    override def write(key: Void, value: SimpleFeature): Unit = {
      val partition = storage.schemes.map(_.getPartition(value))
      val state = cache.getOrElseUpdate(partition, newState(partition))
      state.write(key, value)
      counter.increment(1)
    }

    override def close(context: TaskAttemptContext): Unit = {
      cache.foreach { case (partition, state) =>
        logger.debug(s"Closing writer for $partition")
        state.close(context)
        storage.metadata.register(Map(Partition(partition) -> state.getFiles()))
      }
      CloseWithLogging(Seq(storage, catalog))
    }

    private sealed abstract class PartitionState(partition: Partition) {

      import scala.collection.JavaConverters._

      private val files = new CopyOnWriteArrayList[URI]()
      private val counter = context.getCounter(Group, StorageConfiguration.Counters.partition(partition.toString))

      private var currentFile: Path = _
      private var featureCount: Long = 0

      // noinspection AccessorLikeMethodIsEmptyParen
      def getFiles(): Seq[URI] = files.asScala.toSeq

      def write(key: Void, value: SimpleFeature): Unit = featureCount += 1

      def close(context: TaskAttemptContext): Unit = closeCurrentFile()

      protected def newWriter(): (Path, RecordWriter[Void, SimpleFeature]) = {
        closeCurrentFile()
        // TODO is this in the right fs?
        currentFile = new Path(workPath, FileSystemStorage.newFilePath(storage.sft.getTypeName))
        featureCount = 0
        logger.debug(s"Creating record writer at path $currentFile")
        // noinspection LanguageFeature
        (currentFile, delegate.getRecordWriter(context, currentFile))
      }

      private def closeCurrentFile(): Unit = {
        if (currentFile != null) {
          files.add(currentFile.toUri)
          counter.increment(featureCount)
          currentFile = null
          featureCount = 0
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
          val length = storage.table.io().newInputFile(path.toString).getLength
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

