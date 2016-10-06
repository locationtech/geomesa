/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.ingest

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Job, JobStatus, Mapper}
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.jobs.{GeoMesaConfigurator, JobUtils}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

/**
 * Abstract class that handles configuration and tracking of the remote job
 */
abstract class AbstractIngestJob extends LazyLogging {

  def inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]]
  def configureJob(job: Job): Unit
  def written(job: Job): Long
  def failed(job: Job): Long

  def run(dsParams: Map[String, String],
          typeName: String,
          paths: Seq[String],
          statusCallback: (Float, Long, Long, Boolean) => Unit = (_, _, _, _) => Unit): (Long, Long) = {

    val job = Job.getInstance(new Configuration, "GeoMesa Tools Ingest")

    JobUtils.setLibJars(job.getConfiguration, ingestLibJars, ingestJarSearchPath)

    job.setJarByClass(getClass)
    job.setMapperClass(classOf[IngestMapper])
    job.setInputFormatClass(inputFormatClass)
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

    FileInputFormat.setInputPaths(job, paths.mkString(","))
    configureJob(job)

    GeoMesaConfigurator.setFeatureTypeOut(job.getConfiguration, typeName)
    GeoMesaOutputFormat.configureDataStore(job, dsParams)

    logger.info("Submitting job - please wait...")
    job.submit()
    logger.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        statusCallback(job.mapProgress(), written(job), failed(job), false) // we don't have any reducers, just track mapper progress
      }
      Thread.sleep(1000)
    }
    statusCallback(job.mapProgress(), written(job), failed(job), true)

    if (!job.isSuccessful) {
      logger.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
    }

    (written(job), failed(job))
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
      () => ClassPathUtils.getJarsFromSystemProperty("geomesa.convert.scripts.path"),
      () => ClassPathUtils.getJarsFromClasspath(getClass),
      () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[Connector]))

}

/**
 * Takes the input and writes it to the output - all our main work is done in the input format
 */
class IngestMapper extends Mapper[LongWritable, SimpleFeature, Text, SimpleFeature] with LazyLogging {

  type Context = Mapper[LongWritable, SimpleFeature, Text, SimpleFeature]#Context

  private val text: Text = new Text

  override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
    logger.debug(s"map key ${key.toString}, map value ${DataUtilities.encodeFeature(sf)}")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    context.write(text, sf)
  }
}
