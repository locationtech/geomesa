package org.locationtech.geomesa.tools.ingest

import java.io.File

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.{GeoMesaConfigurator, JobUtils}
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import ConverterInputFormat.{Counters => C }

import scala.collection.JavaConversions._

object ConverterIngestJob extends LazyLogging {

  def run(dsParams: Map[String, String],
          sft: SimpleFeatureType,
          converterConfig: Config,
          paths: Seq[String]): (Long, Long) = {
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

    logger.info("Submitting geomesa convert job")
    job.submit()
    job.waitForCompletion(true)

    val success = job.getCounters.findCounter(C.Group, C.Success).getValue
    val failed = job.getCounters.findCounter(C.Group, C.Failure).getValue
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

class ConvertMapper extends Mapper[LongWritable, SimpleFeature, Text, SimpleFeature] with LazyLogging {

  type Context = Mapper[LongWritable, SimpleFeature, Text, SimpleFeature]#Context

  private val text: Text = new Text
  private var written: Counter = null

  override def setup(context: Context) = {
    written = context.getCounter(C.Group, C.Written)
  }

  override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
    logger.info(s"map key ${key.toString}")
    context.write(text, sf)
    written.increment(1)
  }
}
