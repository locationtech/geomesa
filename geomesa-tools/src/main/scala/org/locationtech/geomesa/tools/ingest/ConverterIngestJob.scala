/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.geotools.data.DataUtilities
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.mapreduce.{ConverterInputFormat, GeoMesaOutputFormat, JobWithLibJars}
import org.locationtech.geomesa.jobs.{Awaitable, JobResult, StatusCallback}
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestCounters
import org.locationtech.geomesa.tools.utils.JobRunner
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.File

/**
 * Distributed job that uses converters to process input files
 *
 * @param dsParams data store connection
 * @param sft simple feature type
 * @param converterConfig converter definition
 * @param paths input files
 * @param libjarsFiles lib jars
 * @param libjarsPaths lib jars paths
 */
class ConverterIngestJob(
    dsParams: Map[String, String],
    sft: SimpleFeatureType,
    converterConfig: Config,
    paths: Seq[String],
    libjarsFiles: Seq[String],
    libjarsPaths: Iterator[() => Seq[File]]
  ) extends Awaitable with JobWithLibJars {

  import ConverterInputFormat.ConverterCounters
  import GeoMesaOutputFormat.OutputCounters

  private val failCounters =
    Seq((ConverterCounters.Group, ConverterCounters.Failed), (OutputCounters.Group, OutputCounters.Failed))

  private val job = {
    val job = Job.getInstance(new Configuration, "GeoMesa Tools Ingest")
    setLibJars(job, libjarsFiles, libjarsPaths)
    configureJob(job)
    JobRunner.submit(job)
    job
  }

  protected def mapCounters(job: Job): Seq[(String, Long)] = {
    Seq(
      (IngestCounters.Ingested, job.getCounters.findCounter(OutputCounters.Group, OutputCounters.Written).getValue),
      (IngestCounters.Failed, failCounters.map(c => job.getCounters.findCounter(c._1, c._2).getValue).sum)
    )
  }

  protected def reduceCounters(job: Job): Seq[(String, Long)] = Seq.empty

  override def await(reporter: StatusCallback): JobResult =
    JobRunner.monitor(job, reporter, mapCounters(job), reduceCounters(job))

  def configureJob(job: Job): Unit = {
    job.setJarByClass(getClass)
    job.setMapperClass(classOf[ConverterIngestJob.IngestMapper])
    job.setInputFormatClass(classOf[ConverterInputFormat])
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

    FileInputFormat.setInputPaths(job, paths.mkString(","))
    ConverterInputFormat.setConverterConfig(job, converterConfig.root().render(ConfigRenderOptions.concise()))
    ConverterInputFormat.setSft(job, sft)

    GeoMesaOutputFormat.setOutput(job.getConfiguration, dsParams, sft)
  }
}

object ConverterIngestJob {

  /**
   * Takes the input and writes it to the output - all our main work is done in the input format
   */
  class IngestMapper extends Mapper[LongWritable, SimpleFeature, Text, SimpleFeature] with LazyLogging {

    type Context = Mapper[LongWritable, SimpleFeature, Text, SimpleFeature]#Context

    private val text: Text = new Text

    override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
      logger.debug(s"map key ${key.toString}, map value ${DataUtilities.encodeFeature(sf)}")
      context.write(text, sf)
    }
  }
}
