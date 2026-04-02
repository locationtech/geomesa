/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.features.SerializationOption
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.Partition
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.{DummyReducer, FsIngestMapper}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat.OutputCounters
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.ConverterIngestJob
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestCounters
import org.locationtech.geomesa.tools.utils.DistributedCopy

import java.io.File

abstract class FileSystemConverterJob(
    dsParams: Map[String, String],
    sft: SimpleFeatureType,
    converterConfig: Config,
    paths: Seq[String],
    libjarsFiles: Seq[String],
    libjarsPaths: Iterator[() => Seq[File]],
    reducers: Int,
    root: Path,
    schemes: Set[PartitionScheme],
    tmpPath: Option[Path],
    targetFileSize: Option[Long]
  ) extends ConverterIngestJob(dsParams, sft, converterConfig, paths, libjarsFiles, libjarsPaths)
    with StorageConfiguration with LazyLogging {

  override protected def reduceCounters(job: Job): Seq[(String, Long)] =
    Seq((IngestCounters.Persisted, job.getCounters.findCounter(OutputCounters.Group, "reduced").getValue))

  override def configureJob(job: Job): Unit = {
    super.configureJob(job)

    job.setMapperClass(classOf[FsIngestMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[BytesWritable])
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[SimpleFeature])
    job.setReducerClass(classOf[DummyReducer])
    job.setNumReduceTasks(reducers)
    // Ensure that the reducers don't start too early
    // (default is at 0.05 which takes all the map slots and isn't needed)
    job.getConfiguration.set("mapreduce.job.reduce.slowstart.completedmaps", ".90")

    StorageConfiguration.setRootPath(job.getConfiguration, root)
    StorageConfiguration.setPartitionScheme(job.getConfiguration, schemes)
    StorageConfiguration.setFileType(job.getConfiguration, FileType.Written)
    targetFileSize.foreach(StorageConfiguration.setTargetFileSize(job.getConfiguration, _))

    FileOutputFormat.setOutputPath(job, tmpPath.getOrElse(root))

    configureOutput(sft, job)
  }

  override def await(reporter: StatusCallback): JobResult = {
    super.await(reporter).merge {
      tmpPath.map { tp =>
        reporter.reset()
        new DistributedCopy().copy(Seq(tp), root, reporter) match {
          case JobSuccess(message, counts) =>
            Command.user.info(message)
            JobSuccess("", counts)

          case j => j
        }
      }
    }
  }
}

object FileSystemConverterJob {

  class ParquetConverterJob(
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      libjarsFiles: Seq[String],
      libjarsPaths: Iterator[() => Seq[File]],
      reducers: Int,
      root: Path,
      schemes: Set[PartitionScheme],
      tmpPath: Option[Path],
      targetFileSize: Option[Long]
    ) extends FileSystemConverterJob(
        dsParams, sft, converterConfig, paths, libjarsFiles, libjarsPaths, reducers, root, schemes, tmpPath, targetFileSize)
          with ParquetStorageConfiguration

  class FsIngestMapper extends Mapper[LongWritable, SimpleFeature, Text, BytesWritable] with LazyLogging {

    type Context = Mapper[LongWritable, SimpleFeature, Text, BytesWritable]#Context

    private var serializer: KryoFeatureSerializer = _
    private var scheme: Set[PartitionScheme] = _

    var mapped: Counter = _
    var written: Counter = _
    var failed: Counter = _

    override def setup(context: Context): Unit = {
      val sft = StorageConfiguration.getSft(context.getConfiguration)
      serializer = KryoFeatureSerializer(sft, SerializationOption.defaults)
      scheme = StorageConfiguration.getPartitionScheme(context.getConfiguration, sft)

      mapped = context.getCounter(OutputCounters.Group, "mapped")
      written = context.getCounter(OutputCounters.Group, OutputCounters.Written)
      failed = context.getCounter(OutputCounters.Group, OutputCounters.Failed)
    }

    override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
      // partitionKey is important because this needs to be correct for the parquet file
      try {
        mapped.increment(1)
        val sfWithFid = GeoMesaFeatureWriter.featureWithFid(sf)
        val partitionKey = new Text(Partition(scheme.map(_.getPartition(sfWithFid))).encoded)
        context.write(partitionKey, new BytesWritable(serializer.serialize(sfWithFid)))
        written.increment(1)
      } catch {
        case e: Throwable =>
          logger.error(s"Failed to write '${DataUtilities.encodeFeature(sf)}'", e)
          failed.increment(1)
      }
    }
  }

  class DummyReducer extends Reducer[Text, BytesWritable, Void, SimpleFeature] {

    import scala.collection.JavaConverters._

    type Context = Reducer[Text, BytesWritable, Void, SimpleFeature]#Context

    private var serializer: KryoFeatureSerializer = _
    private var reduced: Counter = _

    override def setup(context: Context): Unit = {
      val sft = StorageConfiguration.getSft(context.getConfiguration)
      serializer = KryoFeatureSerializer(sft, SerializationOption.defaults)
      reduced = context.getCounter(OutputCounters.Group, "reduced")
    }

    override def reduce(key: Text, values: java.lang.Iterable[BytesWritable], context: Context): Unit = {
      values.asScala.foreach { bw =>
        val sf = serializer.deserialize(bw.getBytes)
        context.write(null, sf)
        reduced.increment(1)
      }
    }
  }
}
