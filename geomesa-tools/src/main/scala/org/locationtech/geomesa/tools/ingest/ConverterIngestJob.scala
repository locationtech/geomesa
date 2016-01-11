/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.ingest

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.convert.{SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.jobs.{GeoMesaConfigurator, JobUtils}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object ConverterIngestJob extends LazyLogging {

  protected[ingest] val ConverterKey = "org.locationtech.geomesa.ingest.converter"
  protected[ingest] val CounterGroup = "org.locationtech.geomesa.convert"

  def run(dsParams: Map[String, String], sft: SimpleFeatureType, converterConfig: Config, paths: Seq[String]): (Long, Long) = {
    val job = Job.getInstance(new Configuration(), s"GeoMesa Ingest")

    JobUtils.setLibJars(job.getConfiguration, libJars = ingestLibJars, searchPath = ingestJarSearchPath)

    // TODO use whole file input format if converter type is xml
    job.setJarByClass(ConverterIngestJob.getClass)
    job.setMapperClass(classOf[ConverterMapper])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[ScalaSimpleFeature])
    job.setNumReduceTasks(0)

    FileInputFormat.setInputPaths(job, paths.mkString(","))
    job.getConfiguration.set(ConverterKey, converterConfig.root().render(ConfigRenderOptions.concise()))
    GeoMesaConfigurator.setFeatureTypeOut(job.getConfiguration, sft.getTypeName)
    GeoMesaOutputFormat.configureDataStore(job, dsParams)

    val result = job.waitForCompletion(true)
    val success = job.getCounters.findCounter(CounterGroup, "success").getValue
    val failed = job.getCounters.findCounter(CounterGroup, "failed").getValue
    (success, failed)
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

class ConverterMapper extends Mapper[LongWritable, Text, Text, SimpleFeature] {

  type Context = Mapper[LongWritable, Text, Text, SimpleFeature]#Context

  private val text: Text = new Text
  private var written: Counter = null
  private var success: Counter = null
  private var failed: Counter = null

  private var sft: SimpleFeatureType = null
  private var converter: SimpleFeatureConverter[String] = null
  private var ec: EvaluationContext = null

  override protected def setup(context: Context): Unit = {
    // TODO add filename to ec
    written = context.getCounter(ConverterIngestJob.CounterGroup, "features-written")
    success = context.getCounter(ConverterIngestJob.CounterGroup, "success")
    failed = context.getCounter(ConverterIngestJob.CounterGroup, "failed")
    val dsParams = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration)
    val ds = DataStoreFinder.getDataStore(dsParams)
    sft = ds.getSchema(GeoMesaConfigurator.getFeatureTypeOut(context.getConfiguration))
    val converterConf = ConfigFactory.parseString(context.getConfiguration.get(ConverterIngestJob.ConverterKey))
    converter = SimpleFeatureConverters.build[String](sft, converterConf)
    ec = converter.createEvaluationContext()
  }

  override protected def cleanup(context: Context): Unit = {
    success.increment(ec.counter.getSuccess)
    failed.increment(ec.counter.getFailure)
  }

  override def map(key: LongWritable, value: Text, context: Context): Unit = {
    converter.processInput(Iterator(value.toString), ec).foreach { sf =>
      context.write(text, sf)
      written.increment(1)
    }
  }
}