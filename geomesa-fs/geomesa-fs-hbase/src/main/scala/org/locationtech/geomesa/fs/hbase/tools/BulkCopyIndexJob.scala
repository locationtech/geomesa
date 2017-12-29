/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.hbase.tools

import java.io.File
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, JobStatus}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.geotools.data.{DataStoreFinder, Query}
import org.locationtech.geomesa.fs.tools.compact.PartitionInputFormat
import org.locationtech.geomesa.fs.tools.ingest.ParquetJobUtils
import org.locationtech.geomesa.fs.{FileSystemDataStore, FileSystemDataStoreParams, FsQueryPlanning}
import org.locationtech.geomesa.hbase.jobs.HBaseIndexFileMapper
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.parquet.SimpleFeatureReadSupport
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class BulkCopyIndexJob {

  def run(fsParams: Map[String, String],
          hbaseParams: Map[String, String],
          typeName: String,
          filter: Option[Filter],
          index: String,
          output: Path,
          tempDir: Option[Path],
          libjarsFile: String,
          libjarsPaths: Iterator[() => Seq[File]],
          statusCallback: StatusCallback): (Boolean, Long, Long) = {

    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._

    val job = Job.getInstance(new Configuration, s"GeoMesa Bulk HBase Load ($index)")

    val fsDs = DataStoreFinder.getDataStore(fsParams).asInstanceOf[FileSystemDataStore]

    try {
      val sft = fsDs.getSchema(typeName)

      JobUtils.setLibJars(job.getConfiguration, readLibJars(libjarsFile), defaultSearchPath ++ libjarsPaths)
      job.setJarByClass(this.getClass)

      // general options
      job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
      job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

      // input format
      job.setInputFormatClass(classOf[PartitionInputFormat])
      ParquetInputFormat.setReadSupportClass(job, classOf[SimpleFeatureReadSupport])
      PartitionInputFormat.setFsPath(job.getConfiguration, FileSystemDataStoreParams.PathParam.lookup(fsParams))
      PartitionInputFormat.setFsEncoding(job.getConfiguration, FileSystemDataStoreParams.EncodingParam.lookup(fsParams))
      val partitions = filter match {
        case None => fsDs.storage.getMetadata(typeName).getPartitions.asScala.toArray
        case Some(f) =>
          FsQueryPlanning.getPartitionsForQuery(fsDs.storage, sft, new Query(typeName, f))
              .intersect(fsDs.storage.getMetadata(typeName).getPartitions).toArray
      }
      PartitionInputFormat.setPartitions(job.getConfiguration, partitions)
      ParquetJobUtils.setSimpleFeatureType(job.getConfiguration, sft)

      // mappers, reducers and output format
      HBaseIndexFileMapper.configure(job, hbaseParams, typeName, index, tempDir.getOrElse(output))

      Command.user.info("Submitting job - please wait...")
      job.submit()
      Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

      def mapCounters = Seq(("written", written(job)), ("failed", failed(job)))

      val stageCount = if (tempDir.isDefined) { 3 } else { 2 }
      var reducing = false

      while (!job.isComplete) {
        Thread.sleep(1000)
        if (job.getStatus.getState != JobStatus.State.PREP) {
          if (reducing) {
            // TODO we could maybe hook into context.set/getStatus, which is used by the HFileReducer instead of counters
            statusCallback(s"Reduce (stage 2/$stageCount): ", job.reduceProgress(), Seq.empty, done = false)
          } else {
            val mapProgress = job.mapProgress()
            if (mapProgress < 1f) {
              statusCallback(s"Map (stage 1/$stageCount): ", mapProgress, mapCounters, done = false)
            } else {
              statusCallback(s"Map (stage 1/$stageCount): ", mapProgress, mapCounters, done = true)
              statusCallback.reset()
              reducing = true
            }
          }
        }
      }
      statusCallback(s"Reduce (stage 2/$stageCount): ", job.reduceProgress(), Seq.empty, done = true)

      val counterResult = (written(job), failed(job))

      val ok = if (!job.isSuccessful) {
        Command.user.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
        false
      } else {
        tempDir.forall(ParquetJobUtils.distCopy(_, output, sft, job.getConfiguration, statusCallback, 3, stageCount))
      }

      (ok, counterResult._1, counterResult._2)
    } finally {
      fsDs.dispose()
    }
  }

  protected def readLibJars(file: String): java.util.List[String] = {
    val is = getClass.getClassLoader.getResourceAsStream(file)
    try {
      IOUtils.readLines(is, StandardCharsets.UTF_8)
    } catch {
      case NonFatal(e) => throw new Exception("Error reading ingest libjars", e)
    } finally {
      IOUtils.closeQuietly(is)
    }
  }

  protected def defaultSearchPath: Iterator[() => Seq[File]] =
    Iterator(
      () => ClassPathUtils.getJarsFromClasspath(getClass),
      () => ClassPathUtils.getFilesFromSystemProperty("geomesa.convert.scripts.path")
    )

  def written(job: Job): Long =
    job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written).getValue

  def failed(job: Job): Long =
    job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed).getValue
}
