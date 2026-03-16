/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.Partition
import org.locationtech.geomesa.fs.storage.common.SizeableFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.fs.tools.compact.FileSystemCompactionJob.CompactionMapper
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat.OutputCounters
import org.locationtech.geomesa.jobs.mapreduce.JobWithLibJars
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.utils.{DistributedCopy, JobRunner}
import org.locationtech.geomesa.utils.text.TextTools

import java.io.File

trait FileSystemCompactionJob extends StorageConfiguration with JobWithLibJars {

  import FileSystemCompactionJob.{FailedCounter, MappedCounter}

  def run(
      storage: FileSystemStorage,
      partitions: Seq[Partition],
      targetFileSize: Option[Long],
      tempPath: Option[Path],
      libjarsFiles: Seq[String],
      libjarsPaths: Iterator[() => Seq[File]],
      statusCallback: StatusCallback): JobResult = {

    val job = Job.getInstance(new Configuration(storage.context.conf), "GeoMesa Storage Compaction")

    setLibJars(job, libjarsFiles, libjarsPaths)
    job.setJarByClass(this.getClass)

    // InputFormat and Mappers
    job.setInputFormatClass(classOf[PartitionInputFormat])
    job.setMapperClass(classOf[CompactionMapper])

    // No reducers - Mapper will read/write its own things
    job.setNumReduceTasks(0)

    job.setMapOutputKeyClass(classOf[Void])
    job.setMapOutputValueClass(classOf[SimpleFeature])
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[SimpleFeature])

    val qualifiedTempPath = tempPath.map(storage.context.fs.makeQualified)

    StorageConfiguration.setRootPath(job.getConfiguration, storage.context.root)
    // TODO ?
    StorageConfiguration.setPartitions(job.getConfiguration, partitions)
    StorageConfiguration.setFileType(job.getConfiguration, FileType.Compacted)
    targetFileSize.foreach(StorageConfiguration.setTargetFileSize(job.getConfiguration, _))

    FileOutputFormat.setOutputPath(job, qualifiedTempPath.getOrElse(storage.context.root))

    // MapReduce options
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

    configureOutput(storage.metadata.sft, job)

    // save the existing files so we can delete them afterwards
    // mimic the filtering done in PartitionInputFormat
    val sizeable = Option(storage).collect { case s: SizeableFileSystemStorage => s }
    val sizeCheck = sizeable.flatMap(s => s.targetSize(targetFileSize).map(t => (p: Path) => s.fileIsSized(p, t)))
    val existingDataFiles = partitions.toList.flatMap { p =>
      val files = storage.metadata.getFiles(p).filterNot(f => sizeCheck.exists(_.apply(new Path(storage.context.root, f.file))))
      // TODO get counts right... use m/r counters?
      if (files.isEmpty) { None } else { Some(p -> files) }
    }

    def mapCounters = Seq((MappedCounter, written(job)), (FailedCounter, failed(job)))

    val result = JobRunner.run(job, statusCallback, mapCounters, Seq.empty).merge {
      qualifiedTempPath.map { tp =>
        new DistributedCopy().copy(Seq(tp), storage.context.root, statusCallback)
      }
    }

    result match {
      case JobSuccess(message, counts) =>
        if (message.nonEmpty) {
          Command.user.info(message)
        }
        Command.user.info("Removing old files")
        existingDataFiles.foreach { case (partition, files) =>
          val name = partition.values.map(_.name).mkString(",")
          val counter = StorageConfiguration.Counters.partition(name)
          val count = Option(job.getCounters.findCounter(StorageConfiguration.Counters.Group, counter)).map(_.getValue)
          files.foreach { f =>
            storage.metadata.removeFile(f)
            storage.context.fs.delete(new Path(storage.context.root, f.file), false)
          }
          val removed = count.map(c => s"containing $c features ").getOrElse("")
          Command.user.info(s"Removed ${TextTools.getPlural(files.size, "file")} ${removed}in partition $name")
        }
        JobSuccess("", counts)

      case j => j
    }
  }

  private def written(job: Job): Long =
    job.getCounters.findCounter(OutputCounters.Group, OutputCounters.Written).getValue

  private def failed(job: Job): Long =
    job.getCounters.findCounter(OutputCounters.Group, OutputCounters.Failed).getValue
}

object FileSystemCompactionJob {

  val MappedCounter = "mapped"
  val FailedCounter = "failed"

  class ParquetCompactionJob extends FileSystemCompactionJob with ParquetStorageConfiguration

  /**
    * Mapper that simply reads the input format and writes the output to the sample node. This mapper
    * is paired with the PartitionRecordReader which will feed all the features into a single map task
    */
  class CompactionMapper extends Mapper[Void, SimpleFeature, Void, SimpleFeature] with LazyLogging {

    type Context = Mapper[Void, SimpleFeature, Void, SimpleFeature]#Context

    private var written: Counter = _
    private var mapped: Counter = _

    override def setup(context: Context): Unit = {
      super.setup(context)
      written = context.getCounter(OutputCounters.Group, OutputCounters.Written)
      mapped = context.getCounter("org.locationtech.geomesa.fs.compaction", MappedCounter)
    }

    override def map(key: Void, sf: SimpleFeature, context: Context): Unit = {
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      mapped.increment(1)
      context.write(null, sf)
      written.increment(1)
    }
  }
}