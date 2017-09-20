/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.hadoop.{ParquetOutputCommitter, ParquetOutputFormat}
import org.locationtech.geomesa.fs.storage.common.FileType.FileType
import org.locationtech.geomesa.fs.storage.common.{FileType, StorageUtils}
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable

class SchemeOutputFormat extends ParquetOutputFormat[SimpleFeature] {

  val extension = ".parquet" // TODO read this from configuration
  private var commiter: SchemeOutputCommitter = _

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    if (commiter == null) {
      val output = FileOutputFormat.getOutputPath(context)
      commiter = new SchemeOutputCommitter(extension, output, context)
    }
    commiter
  }

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, SimpleFeature] = {

    val sft = ParquetJobUtils.getSimpleFeatureType(context.getConfiguration)
    val name = sft.getTypeName
    val conf = context.getConfiguration
    val fileType: FileType = SchemeOutputFormat.getFileType(context.getConfiguration)

    new RecordWriter[Void, SimpleFeature] with LazyLogging {

      private val partitionScheme = org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft)

      var curPartition: String = _
      var writer: RecordWriter[Void, SimpleFeature] = _
      var sentToParquet: Counter = context.getCounter(GeoMesaOutputFormat.Counters.Group, "sentToParquet")

      override def write(key: Void, value: SimpleFeature): Unit = {
        val keyPartition = partitionScheme.getPartitionName(value)

        def initWriter() = {
          val committer = getOutputCommitter(context).asInstanceOf[FileOutputCommitter]
          val root = committer.getWorkPath
          val fs = root.getFileSystem(conf)
          // TODO combine this with the same code in ParquetFileSystemStorage
          val file = StorageUtils.nextFile(fs, root, name, keyPartition, partitionScheme.isLeafStorage, "parquet", fileType)
          logger.info(s"Creating Date scheme record writer at path ${file.toString}")
          curPartition = keyPartition
          writer = getRecordWriter(context, file)
        }

        if (writer == null) {
          initWriter()
        } else if (keyPartition != curPartition) {
          writer.close(context)
          logger.info(s"Closing writer for $curPartition")
          initWriter()
        }
        writer.write(key, value)
        sentToParquet.increment(1)
      }

      override def close(context: TaskAttemptContext): Unit = {
        if (writer != null) writer.close(context)
      }
    }
  }

  override def checkOutputSpecs(job: JobContext): Unit = {
    // Ensure that the output directory is set and not already there
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) throw new InvalidJobConfException("Output directory not set.")
    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }
}

object SchemeOutputFormat {
  val FileTypeParam = "geomesa.fs.mapreduce.output.filetype"
  def getFileType(conf: Configuration): FileType = FileType.withName(conf.get(FileTypeParam))
  def setFileType(conf: Configuration, fileType: FileType): Unit = conf.set(FileTypeParam, fileType.toString)
}

class SchemeOutputCommitter(extension: String,
                            outputPath: Path,
                            context: TaskAttemptContext)
  extends FileOutputCommitter(outputPath, context) with LazyLogging {

  @throws[IOException]
  override def commitJob(jobContext: JobContext) {
    super.commitJob(jobContext)
    val conf = ContextUtil.getConfiguration(jobContext)
    SchemeOutputCommitter.listFiles(outputPath, conf, extension).map(_.getParent).distinct.foreach { path =>
      ParquetOutputCommitter.writeMetaDataFile(conf, path)
      logger.info(s"Wrote metadata file for path $path")
    }
  }
}

object SchemeOutputCommitter {
  def listFiles(path: Path, conf: Configuration, suffix: String): Seq[Path] = {
    val fs = path.getFileSystem(conf)
    val listing = fs.listFiles(path, true)

    val result = mutable.ListBuffer.empty[Path]
    while (listing.hasNext) {
      val next = listing.next()
      if (next.isFile) {
        val p = next.getPath
        if (p.getName.endsWith(suffix)) {
          result += p
        }
      }
    }

    result
  }
}
