/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.File

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Job, JobStatus, Mapper}
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.jobs.{GeoMesaConfigurator, JobUtils}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object ConverterIngestJob extends LazyLogging {

  def run(dsParams: Map[String, String],
          sft: SimpleFeatureType,
          converterConfig: Config,
          paths: Seq[String],
          statusCallback: (Float, Long, Long, Boolean) => Unit = (_, _, _, _) => Unit): (Long, Long) = {
    val job = Job.getInstance(new Configuration, "GeoMesa Converter Ingest")

    JobUtils.setLibJars(job.getConfiguration, libJars = ingestLibJars, searchPath = ingestJarSearchPath)

    job.setJarByClass(ConverterIngestJob.getClass)
    job.setMapperClass(classOf[ConvertMapper])
    job.setInputFormatClass(classOf[ConverterInputFormat])
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")

    FileInputFormat.setInputPaths(job, paths.mkString(","))
    ConverterInputFormat.setConverterConfig(job, converterConfig.root().render(ConfigRenderOptions.concise()))
    ConverterInputFormat.setSft(job, sft)
    GeoMesaConfigurator.setFeatureTypeOut(job.getConfiguration, sft.getTypeName)
    GeoMesaOutputFormat.configureDataStore(job, dsParams)

    job.submit()
    logger.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    import ConverterInputFormat.{Counters => ConvertCounters}
    import GeoMesaOutputFormat.{Counters => OutCounters}

    val failCounters =
      Seq((ConvertCounters.Group, ConvertCounters.Failed), (OutCounters.Group, OutCounters.Failed))

    def written: Long = job.getCounters.findCounter(OutCounters.Group, OutCounters.Written).getValue
    def failed: Long = failCounters.map(c => job.getCounters.findCounter(c._1, c._2).getValue).sum

    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        statusCallback(job.mapProgress(), written, failed, false) // we don't have any reducers, just track mapper progress
      }
      Thread.sleep(1000)
    }
    statusCallback(job.mapProgress(), written, failed, true)

    if (!job.isSuccessful) {
      logger.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
    }

    (written, failed)
  }

  def ingestLibJars = {
    val is = getClass.getClassLoader.getResourceAsStream("org/locationtech/geomesa/tools/ingest-libjars.list")
    try {
      IOUtils.readLines(is)
    } catch {
      case e: Exception => throw new Exception("Error reading ingest libjars", e)
    } finally {
      IOUtils.closeQuietly(is)
    }
  }

  def ingestJarSearchPath: Iterator[() => Seq[File]] =
    Iterator(() => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HOME"),
      () => ClassPathUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
      () => ClassPathUtils.getJarsFromClasspath(getClass),
      () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[Connector]))

}

class ConvertMapper extends Mapper[LongWritable, SimpleFeature, Text, SimpleFeature] with LazyLogging {

  type Context = Mapper[LongWritable, SimpleFeature, Text, SimpleFeature]#Context

  private val text: Text = new Text

  override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
    logger.debug(s"map key ${key.toString}, map value ${DataUtilities.encodeFeature(sf)}")
    context.write(text, sf)
  }
}
