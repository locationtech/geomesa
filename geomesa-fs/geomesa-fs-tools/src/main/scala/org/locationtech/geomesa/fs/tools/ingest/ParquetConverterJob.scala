/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File
import java.lang.Iterable

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.FileType
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.parquet.SimpleFeatureWriteSupport
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.ConverterIngestJob
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class ParquetConverterJob(sft: SimpleFeatureType,
                          converterConfig: Config,
                          dsPath: Path,
                          tempPath: Option[Path],
                          reducers: Int) extends ConverterIngestJob(sft, converterConfig) with LazyLogging {

  override def run(dsParams: Map[String, String],
                   typeName: String,
                   paths: Seq[String],
                   libjarsFile: String,
                   libjarsPaths: Iterator[() => Seq[File]],
                   statusCallback: StatusCallback): (Long, Long) = {

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]
    val job = Job.getInstance(new Configuration, "GeoMesa Parquet Ingest")

    JobUtils.setLibJars(job.getConfiguration, readLibJars(libjarsFile), defaultSearchPath ++ libjarsPaths)

    job.setJarByClass(getClass)
    job.setMapperClass(classOf[IngestMapper])
    job.setInputFormatClass(inputFormatClass)
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[BytesWritable])

    // Dummy reducer to convert to void and shuffle
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[DummyReducer])
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")

    // Ensure that the reducers don't start to early (default is at 0.05 which takes all the map slots and isn't needed)
    job.getConfiguration.set("mapreduce.job.reduce.slowstart.completedmaps", ".90")
    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

    // Output format
    job.setOutputFormatClass(classOf[SchemeOutputFormat])
    SchemeOutputFormat.setFileType(job.getConfiguration, FileType.Written)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[SimpleFeature])

    // Parquet Options
    val summaryLevel = Option(sft.getUserData.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL).asInstanceOf[String])
      .getOrElse(ParquetOutputFormat.JobSummaryLevel.NONE.toString)
    job.getConfiguration.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, summaryLevel)
    Command.user.info(s"Parquet metadata summary level is $summaryLevel")

    val compression = Option(sft.getUserData.get(ParquetOutputFormat.COMPRESSION).asInstanceOf[String])
      .map(CompressionCodecName.valueOf)
      .getOrElse(CompressionCodecName.SNAPPY)
    ParquetOutputFormat.setCompression(job, compression)
    Command.user.info(s"Parquet compression is $compression")

    // More Parquet config
    ParquetOutputFormat.setWriteSupportClass(job, classOf[SimpleFeatureWriteSupport])
    ParquetJobUtils.setSimpleFeatureType(job.getConfiguration, sft)

    FileOutputFormat.setOutputPath(job, tempPath.getOrElse(dsPath))

    FileInputFormat.setInputPaths(job, paths.mkString(","))
    configureJob(job)

    Command.user.info("Submitting job - please wait...")
    job.submit()
    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    def mapCounters = Seq(("mapped", written(job)), ("failed", failed(job)))
    def reduceCounters = Seq(("ingested", reduced(job)))

    val stageCount = if (tempPath.isDefined) { "3" } else { "2" }

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
      Thread.sleep(1000)
    }
    statusCallback(s"Reduce (stage 2/$stageCount): ", job.reduceProgress(), reduceCounters, done = true)

    // Do this earlier than the data copy bc its throwing errors
    val res = (written(job), failed(job))

    val ret = job.isSuccessful &&
        tempPath.forall(tp => ParquetJobUtils.distCopy(tp, dsPath, sft, job.getConfiguration, statusCallback)) && {
      Command.user.info("Attempting to update metadata")
      // We sleep here to allow a chance for S3 to become "consistent" with its storage listings
      Thread.sleep(5000)
      ds.storage.updateMetadata(typeName)
      Command.user.info("Metadata Updated")
      true
    }

    if (!ret) {
      Command.user.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
    }

    res
  }

  def reduced(job: Job): Long = job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, "reduced").getValue
}

class IngestMapper extends Mapper[LongWritable, SimpleFeature, Text, BytesWritable] with LazyLogging {

  type Context = Mapper[LongWritable, SimpleFeature, Text, BytesWritable]#Context

  private var serializer: KryoFeatureSerializer = _
  private var partitionScheme: PartitionScheme = _

  var written: Counter = _
  var failed: Counter = _

  override def setup(context: Context): Unit = {
    super.setup(context)
    val sft = ParquetJobUtils.getSimpleFeatureType(context.getConfiguration)
    serializer = KryoFeatureSerializer(sft, SerializationOptions.withUserData)
    partitionScheme = org.locationtech.geomesa.fs.storage.common.PartitionScheme.extractFromSft(sft)

    written = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written)
    failed = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed)
  }

  override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    context.getCounter("geomesa", "map").increment(1)
    // partitionKey is important because this needs to be correct for the parquet file
    try {
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
    val sft = ParquetJobUtils.getSimpleFeatureType(context.getConfiguration)
    serializer = KryoFeatureSerializer(sft, SerializationOptions.withUserData)
    reduced = context.getCounter(GeoMesaOutputFormat.Counters.Group, "reduced")
  }

  override def reduce(key: Text, values: Iterable[BytesWritable], context: Context): Unit = {
    values.foreach { bw =>
      context.write(null, serializer.deserialize(bw.getBytes))
      reduced.increment(1)
    }
  }

}
