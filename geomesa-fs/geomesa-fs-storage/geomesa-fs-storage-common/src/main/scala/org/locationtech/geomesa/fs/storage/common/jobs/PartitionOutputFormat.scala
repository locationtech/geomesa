/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.fs.storage.common.jobs.PartitionOutputFormat.SingleFileOutputFormat
import org.locationtech.geomesa.fs.storage.common.{PartitionScheme, StorageUtils}
import org.opengis.feature.simple.SimpleFeature

/**
  * Output format that writes to multiple partition files
  *
  * @param delegate underlying output format for a single file
  */
abstract class PartitionOutputFormat(delegate: SingleFileOutputFormat) extends OutputFormat[Void, SimpleFeature] {

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

    private val sft = StorageConfiguration.getSft(context.getConfiguration)
    private val scheme = PartitionScheme.extractFromSft(sft)
    private val encoding = StorageConfiguration.getEncoding(context.getConfiguration)
    private val fileType = StorageConfiguration.getFileType(context.getConfiguration)

    private val counter = context.getCounter(Group, Features)
    private val cache = scala.collection.mutable.Map.empty[String, RecordWriter[Void, SimpleFeature]]

    override def write(key: Void, value: SimpleFeature): Unit = {
      val partition = scheme.getPartitionName(value)
      cache.getOrElseUpdate(partition, createWriter(partition)).write(key, value)
      counter.increment(1)
    }

    override def close(context: TaskAttemptContext): Unit = {
      cache.foreach { case (partition, writer) =>
        logger.info(s"Closing writer for $partition")
        writer.close(context)
      }
    }

    protected def getRootPath(context: TaskAttemptContext): Path =
      delegate.getOutputCommitter(context).asInstanceOf[FileOutputCommitter].getWorkPath

    private def createWriter(partition: String): RecordWriter[Void, SimpleFeature] = {
      val root = getRootPath(context)
      val fs = root.getFileSystem(context.getConfiguration)
      // TODO combine this with the same code in ParquetFileSystemStorage
      val file = StorageUtils.nextFile(fs, root, sft.getTypeName, partition, scheme.isLeafStorage, encoding, fileType)
      logger.info(s"Creating ${scheme.name()} scheme record writer at path $file")
      // noinspection LanguageFeature
      delegate.getRecordWriter(context, file)
    }
  }
}

object PartitionOutputFormat {
  type SingleFileOutputFormat = FileOutputFormat[Void, SimpleFeature] {
    def getRecordWriter(context: TaskAttemptContext, file: Path): RecordWriter[Void, SimpleFeature]
  }
}