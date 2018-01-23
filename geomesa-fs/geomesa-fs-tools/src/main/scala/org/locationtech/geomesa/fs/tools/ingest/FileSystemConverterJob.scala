/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File
import java.lang.Iterable

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.{FileSystemStorageFactory, FileType}
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcStorageConfiguration
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.{DummyReducer, IngestMapper}
import org.locationtech.geomesa.jobs.mapreduce.{ConverterInputFormat, GeoMesaOutputFormat, JobWithLibJars}
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.opengis.feature.simple.SimpleFeature

trait FileSystemConverterJob extends StorageConfiguration with JobWithLibJars with LazyLogging {

  def run(dsParams: Map[String, String],
          typeName: String,
          converterConfig: Config,
          inputPaths: Seq[String],
          tempPath: Option[Path],
          reducers: Int,
          libjarsFile: String,
          libjarsPaths: Iterator[() => Seq[File]],
          statusCallback: StatusCallback): (Long, Long) = {

    import scala.collection.JavaConversions._

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]
    try {
      val sft = ds.getSchema(typeName)

      val job = Job.getInstance(new Configuration, "GeoMesa Storage Ingest")

      setLibJars(job, libjarsFile, libjarsPaths)

      job.setJarByClass(this.getClass)

      // InputFormat and Mappers
      job.setInputFormatClass(classOf[ConverterInputFormat])
      job.setMapperClass(classOf[IngestMapper])

      // Dummy reducer to convert to void and shuffle
      job.setReducerClass(classOf[DummyReducer])
      job.setNumReduceTasks(reducers)

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[BytesWritable])
      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(classOf[SimpleFeature])

      StorageConfiguration.setSft(job.getConfiguration, sft)
      StorageConfiguration.setPath(job.getConfiguration, FileSystemStorageFactory.PathParam.lookup(dsParams))
      StorageConfiguration.setEncoding(job.getConfiguration, FileSystemStorageFactory.EncodingParam.lookup(dsParams))
      StorageConfiguration.setFileType(job.getConfiguration, FileType.Written)

      // from ConverterIngestJob
      ConverterInputFormat.setConverterConfig(job, converterConfig.root().render(ConfigRenderOptions.concise()))
      ConverterInputFormat.setSft(job, sft)

      FileInputFormat.setInputPaths(job, inputPaths.mkString(","))
      FileOutputFormat.setOutputPath(job, tempPath.getOrElse(new Path(ds.storage.getRoot)))

      // MapReduce options
      job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
      job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
      job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")
      // Ensure that the reducers don't start too early (default is at 0.05 which takes all the map slots and isn't needed)
      job.getConfiguration.set("mapreduce.job.reduce.slowstart.completedmaps", ".90")

      configureOutput(sft, job)

      Command.user.info("Submitting job - please wait...")
      job.submit()
      Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

      def mapCounters = Seq(("mapped", written(job)), ("failed", failed(job)))
      def reduceCounters = Seq(("written", reduced(job)))

      val stageCount = if (tempPath.isDefined) { 3 } else { 2 }

      var mapping = true
      while (!job.isComplete) {
        if (job.getStatus.getState != JobStatus.State.PREP) {
          if (mapping) {
            val mapProgress = job.mapProgress()
            if (mapProgress < 1f) {
              statusCallback(s"Map (stage 1/$stageCount): ", mapProgress, mapCounters, done = false)
            } else {
              statusCallback(s"Map (stage 1/$stageCount): ", mapProgress, mapCounters, done = true)
              statusCallback.reset()
              mapping = false
            }
          } else {
            statusCallback(s"Reduce (stage 2/$stageCount): ", job.reduceProgress(), reduceCounters, done = false)
          }
        }
        Thread.sleep(500)
      }
      statusCallback(s"Reduce (stage 2/$stageCount): ", job.reduceProgress(), reduceCounters, done = true)

      // Do this earlier than the data copy bc its throwing errors
      val counterResult = (written(job), failed(job))

      if (!job.isSuccessful) {
        Command.user.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
      } else {
        val copied = tempPath.forall { tp =>
          val p = new Path(ds.storage.getRoot)
          StorageJobUtils.distCopy(tp, p, sft, statusCallback, 2, stageCount)
        }
        if (copied) {
          Command.user.info("Attempting to update metadata")
          // We sleep here to allow a chance for S3 to become "consistent" with its storage listings
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

  def written(job: Job): Long = job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written).getValue
  def failed(job: Job): Long = job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed).getValue
  def reduced(job: Job): Long = job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, "reduced").getValue
}

object FileSystemConverterJob {

  class ParquetConverterJob extends FileSystemConverterJob with ParquetStorageConfiguration

  class OrcConverterJob extends FileSystemConverterJob with OrcStorageConfiguration

  class IngestMapper extends Mapper[LongWritable, SimpleFeature, Text, BytesWritable] with LazyLogging {

    type Context = Mapper[LongWritable, SimpleFeature, Text, BytesWritable]#Context

    private var serializer: KryoFeatureSerializer = _
    private var partitionScheme: PartitionScheme = _

    var mapped: Counter = _
    var written: Counter = _
    var failed: Counter = _

    override def setup(context: Context): Unit = {
      super.setup(context)
      val sft = StorageConfiguration.getSft(context.getConfiguration)
      serializer = KryoFeatureSerializer(sft, SerializationOptions.none)
      partitionScheme = org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft)

      mapped = context.getCounter(GeoMesaOutputFormat.Counters.Group, "mapped")
      written = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written)
      failed = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed)
    }

    override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
      // partitionKey is important because this needs to be correct for the parquet file
      try {
        mapped.increment(1)
        val partitionKey = new Text(partitionScheme.getPartitionName(sf))
        context.write(partitionKey, new BytesWritable(serializer.serialize(sf)))
        written.increment(1)
      } catch {
        case e: Throwable =>
          logger.error(s"Failed to write '${DataUtilities.encodeFeature(sf)}'", e)
          failed.increment(1)
      }
    }
  }

  class DummyReducer extends Reducer[Text, BytesWritable, Void, SimpleFeature] {

    type Context = Reducer[Text, BytesWritable, Void, SimpleFeature]#Context

    private var serializer: KryoFeatureSerializer = _
    var reduced: Counter = _

    override def setup(context: Context): Unit = {
      super.setup(context)
      val sft = StorageConfiguration.getSft(context.getConfiguration)
      serializer = KryoFeatureSerializer(sft, SerializationOptions.none)
      reduced = context.getCounter(GeoMesaOutputFormat.Counters.Group, "reduced")
    }

    override def reduce(key: Text, values: Iterable[BytesWritable], context: Context): Unit = {
      import scala.collection.JavaConversions._
      values.foreach { bw =>
        context.write(null, serializer.deserialize(bw.getBytes))
        reduced.increment(1)
      }
    }

  }
}
