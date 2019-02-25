/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileContext, Path}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata}
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, StorageMetadataFactory}
import org.locationtech.geomesa.fs.storage.common.jobs.PartitionOutputFormat.{PartitionState, SingleFileOutputFormat}
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.opengis.feature.simple.SimpleFeature

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

    // note: don't invoke 'reload' - the metadata may not have any partition data, but we only use it for writing
    private val metadata = {
      val conf = context.getConfiguration
      val root = StorageConfiguration.getRootPath(conf)
      val fsc = FileSystemContext(FileContext.getFileContext(root.toUri, conf), conf, root)
      StorageMetadataFactory.load(fsc).getOrElse {
        throw new IllegalArgumentException(s"No storage defined under path '$root'")
      }
    }
    private val fileType = StorageConfiguration.getFileType(context.getConfiguration)

    private val counter = context.getCounter(Group, Features)
    private val cache = scala.collection.mutable.Map.empty[String, PartitionState]

    override def write(key: Void, value: SimpleFeature): Unit = {
      val partition = metadata.scheme.getPartitionName(value)
      val state = cache.getOrElseUpdate(partition, createWriter(partition))
      state.writer.write(key, value)
      val geom = value.getDefaultGeometry.asInstanceOf[Geometry]
      if (geom != null) {
        state.bounds.expandToInclude(geom.getEnvelopeInternal)
      }
      state.count += 1L
      counter.increment(1)
    }

    override def close(context: TaskAttemptContext): Unit = {
      cache.foreach { case (partition, state) =>
        logger.debug(s"Closing writer for $partition")
        state.writer.close(context)
        val meta = PartitionMetadata(partition, Seq(state.file), PartitionBounds(state.bounds), state.count)
        metadata.addPartition(meta)
      }
      CloseWithLogging(metadata)
    }

    private def createWriter(partition: String): PartitionState = {
      val root = delegate.getOutputCommitter(context).asInstanceOf[FileOutputCommitter].getWorkPath
      // TODO combine this with the same code in ParquetFileSystemStorage
      val file = StorageUtils.nextFile(root, partition, metadata.leafStorage, metadata.encoding, fileType)
      logger.debug(s"Creating record writer at path $file")
      // noinspection LanguageFeature
      PartitionState(root, file.getName, delegate.getRecordWriter(context, file))
    }
  }
}

object PartitionOutputFormat {

  type SingleFileOutputFormat = FileOutputFormat[Void, SimpleFeature] {
    def getRecordWriter(context: TaskAttemptContext, file: Path): RecordWriter[Void, SimpleFeature]
  }

  private case class PartitionState(root: Path, file: String, writer: RecordWriter[Void, SimpleFeature]) {
    var count = 0L
    val bounds = new Envelope()
  }
}
