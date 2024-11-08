/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata, StorageFile}
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, FileSystemStorageFactory, StorageMetadataFactory}
import org.locationtech.geomesa.fs.storage.common.SizeableFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.jobs.PartitionOutputFormat.SingleFileOutputFormat
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FileSizeEstimator}
import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.collection.mutable.ArrayBuffer

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

  class PartitionSchemeRecordWriter(context: TaskAttemptContext)
      extends RecordWriter[Void, SimpleFeature] with LazyLogging {

    import StorageConfiguration.Counters.{Features, Group}

    private val storage = {
      val conf = context.getConfiguration
      val root = StorageConfiguration.getRootPath(conf)
      val fsc = FileSystemContext(root, conf)
      val metadata = StorageMetadataFactory.load(fsc).getOrElse {
        throw new IllegalArgumentException(s"No storage defined under path '$root'")
      }
      FileSystemStorageFactory(fsc, metadata)
    }
    private val encoding = storage.metadata.encoding
    private val leaf = storage.metadata.leafStorage

    private val fileType = StorageConfiguration.getFileType(context.getConfiguration)
    private val fileSize = StorageConfiguration.getTargetFileSize(context.getConfiguration)

    private val counter = context.getCounter(Group, Features)
    private val cache = scala.collection.mutable.Map.empty[String, PartitionState]

    private val workPath = delegate.getOutputCommitter(context).asInstanceOf[FileOutputCommitter].getWorkPath

    private def newState(partition: String): PartitionState = {
      val estimator = storage match {
        case s: SizeableFileSystemStorage => s.targetSize(fileSize).map(s.estimator)
        case _ => None
      }
      estimator match {
        case None => new SinglePartitionState(partition)
        case Some(e) => new ChunkedPartitionState(partition, e, context)
      }
    }

    override def write(key: Void, value: SimpleFeature): Unit = {
      val partition = storage.metadata.scheme.getPartitionName(value)
      val state = cache.getOrElseUpdate(partition, newState(partition))
      state.write(key, value)
      counter.increment(1)
    }

    override def close(context: TaskAttemptContext): Unit = {
      cache.foreach { case (partition, state) =>
        logger.debug(s"Closing writer for $partition")
        state.close(context)
        storage.metadata.addPartition(state.meta)
      }
      CloseWithLogging(storage)
    }

    sealed abstract class PartitionState(partition: String) {

      private var count = 0L
      private val bounds = new Envelope()
      private val files = ArrayBuffer.empty[String]

      def write(key: Void, value: SimpleFeature): Unit = {
        val geom = value.getDefaultGeometry.asInstanceOf[Geometry]
        if (geom != null) {
          bounds.expandToInclude(geom.getEnvelopeInternal)
        }
        count += 1L
      }

      def meta: PartitionMetadata = {
        val millis = System.currentTimeMillis()
        val f = files.map(StorageFile(_, millis))
        PartitionMetadata(partition, f.toSeq, PartitionBounds(bounds), count)
      }

      def close(context: TaskAttemptContext): Unit =
        context.getCounter(Group, StorageConfiguration.Counters.partition(partition)).increment(count)

      protected def newWriter(): (Path, RecordWriter[Void, SimpleFeature]) = {
        val file = StorageUtils.nextFile(workPath, partition, leaf, encoding, fileType)
        logger.debug(s"Creating record writer at path $file")
        files += file.getName
        // noinspection LanguageFeature
        (file, delegate.getRecordWriter(context, file))
      }
    }

    private class SinglePartitionState(partition: String) extends PartitionState(partition) {

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

    private class ChunkedPartitionState(partition: String, estimator: FileSizeEstimator, context: TaskAttemptContext)
        extends PartitionState(partition) {

      private var count = 0L // number of features written
      private var total = 0L // sum size of all finished chunks
      private var remaining = estimator.estimate(0L)

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
          writer.close(context)
          writer = null
          // adjust our estimate to account for the actual bytes written
          total += storage.context.fs.getFileStatus(path).getLen
          estimator.update(total, count)
          remaining = estimator.estimate(0L)
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
