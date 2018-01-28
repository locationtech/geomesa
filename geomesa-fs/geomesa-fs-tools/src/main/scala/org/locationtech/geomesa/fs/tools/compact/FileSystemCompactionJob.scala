/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.geotools.data.DataStoreFinder
import org.geotools.factory.Hints
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.jobs.{PartitionInputFormat, StorageConfiguration}
import org.locationtech.geomesa.fs.storage.common.{FileSystemStorageFactory, FileType, PartitionScheme}
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcStorageConfiguration
import org.locationtech.geomesa.fs.tools.compact.FileSystemCompactionJob.CompactionMapper
import org.locationtech.geomesa.fs.tools.ingest.StorageJobUtils
import org.locationtech.geomesa.jobs.mapreduce.{GeoMesaOutputFormat, JobWithLibJars}
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.opengis.feature.simple.SimpleFeature

trait FileSystemCompactionJob extends StorageConfiguration with JobWithLibJars {

  def run(dsParams: Map[String, String],
          typeName: String,
          partitions: Seq[String],
          tempPath: Option[Path],
          libjarsFile: String,
          libjarsPaths: Iterator[() => Seq[File]],
          statusCallback: StatusCallback): (Long, Long) = {

    import scala.collection.JavaConversions._

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]
    try {
      val sft = ds.getSchema(typeName)
      // Validate that there is a partition scheme
      PartitionScheme.extractFromSft(sft)

      val job = Job.getInstance(new Configuration, "GeoMesa Storage Compaction")

      setLibJars(job, libjarsFile, libjarsPaths)
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

      StorageConfiguration.setSft(job.getConfiguration, sft)
      StorageConfiguration.setPath(job.getConfiguration, FileSystemStorageFactory.PathParam.lookup(dsParams))
      StorageConfiguration.setEncoding(job.getConfiguration, FileSystemStorageFactory.EncodingParam.lookup(dsParams))
      StorageConfiguration.setPartitions(job.getConfiguration, partitions.toArray)
      StorageConfiguration.setFileType(job.getConfiguration, FileType.Compacted)

      FileOutputFormat.setOutputPath(job, tempPath.getOrElse(new Path(ds.storage.getRoot)))

      // MapReduce options
      job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
      job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

      configureOutput(sft, job)

      // Save the existing files so we can delete them afterwards
      // Be sure to filter this based on the input partitions
      val existingDataFiles = ds.storage.getPartitions(typeName)
          .intersect(partitions).flatMap(ds.storage.getPaths(typeName, _)).toList

      Command.user.info("Submitting job - please wait...")
      job.submit()
      Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

      def mapCounters = Seq(("mapped", written(job)), ("failed", failed(job)))

      val stageCount = if (tempPath.isDefined) { 2 } else { 1 }

      while (!job.isComplete) {
        Thread.sleep(1000)
        if (job.getStatus.getState != JobStatus.State.PREP) {
          val mapProgress = job.mapProgress()
          if (mapProgress < 1f) {
            statusCallback(s"Map (stage 1/$stageCount): ", mapProgress, mapCounters, done = false)
          }
        }
      }
      statusCallback(s"Map (stage 1/$stageCount): ", job.mapProgress(), mapCounters, done = true)
      statusCallback.reset()

      val counterResult = (written(job), failed(job))

      if (!job.isSuccessful) {
        Command.user.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
      } else {
        val copied = tempPath.forall { tp =>
          val p = new Path(ds.storage.getRoot)
          StorageJobUtils.distCopy(tp, p, sft, statusCallback, 2, stageCount)
        }
        if (copied) {
          Command.user.info("Removing old files")
          val fs = new Path(ds.storage.getRoot).getFileSystem(job.getConfiguration)
          existingDataFiles.foreach(o => fs.delete(new Path(o), false))
          Command.user.info(s"Removed ${existingDataFiles.size} files")

          Command.user.info("Updating metadata")
          // TODO GEOMESA-2018 We sleep here to allow a chance for S3 to become "consistent" with its storage listings
          Thread.sleep(5000)
          ds.storage.updateMetadata(typeName)
          Command.user.info("Metadata Updated")
        }
      }

      counterResult
    } finally {
      ds.dispose()
    }
  }

  private def written(job: Job): Long =
    job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written).getValue

  private def failed(job: Job): Long =
    job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed).getValue
}

object FileSystemCompactionJob {

  class ParquetCompactionJob extends FileSystemCompactionJob with ParquetStorageConfiguration

  class OrcCompactionJob extends FileSystemCompactionJob with OrcStorageConfiguration

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
      written = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written)
      mapped = context.getCounter("org.locationtech.geomesa.fs.compaction", "mapped")
    }

    override def map(key: Void, sf: SimpleFeature, context: Context): Unit = {
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      mapped.increment(1)
      context.write(null, sf)
      written.increment(1)
    }
  }
}