/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{File, InputStream}
import java.util.concurrent.atomic.AtomicLong

import com.beust.jcommander.ParameterException
import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.locationtech.geomesa.convert.{DefaultCounter, EvaluationContext, SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.jobs.mapreduce.{ConverterInputFormat, GeoMesaOutputFormat}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.DataUtilities.compare

import scala.util.Try

/**
  * Ingestion that uses geomesa converters to process input files
  *
  * @param sft simple feature type
  * @param dsParams data store parameters
  * @param converterConfig converter definition
  * @param inputs files to ingest
  * @param libjarsFile file with list of jars needed for ingest
  * @param libjarsPaths paths to search for libjars
  * @param numLocalThreads for local ingest, how many threads to use
  */
class ConverterIngest(sft: SimpleFeatureType,
                      dsParams: Map[String, String],
                      converterConfig: Config,
                      inputs: Seq[String],
                      mode: Option[RunMode],
                      libjarsFile: String,
                      libjarsPaths: Iterator[() => Seq[File]],
                      numLocalThreads: Int,
                      configuration: Option[Configuration] = None)
    extends AbstractIngest(dsParams, sft.getTypeName, inputs, mode, libjarsFile, libjarsPaths, numLocalThreads) {

  override def beforeRunTasks(): Unit = {
    // create schema for the feature prior to Ingest job
    val existing = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
    if (existing == null) {
      Command.user.info(s"Creating schema '${sft.getTypeName}'")
      ds.createSchema(sft)
    } else {
      Command.user.info(s"Schema '${sft.getTypeName}' exists")
      if (compare(sft, existing) != 0) {
        throw new ParameterException("Existing simple feature type does not match expected type" +
            s"\n  existing: '${SimpleFeatureTypes.encodeType(existing)}'" +
            s"\n  expected: '${SimpleFeatureTypes.encodeType(sft)}'")
      }
    }
  }

  private val factory = new BasePooledObjectFactory[SimpleFeatureConverter[_]] {
    override def wrap(obj: SimpleFeatureConverter[_]) = new DefaultPooledObject[SimpleFeatureConverter[_]](obj)
    override def create(): SimpleFeatureConverter[_] = SimpleFeatureConverters.build(sft, converterConfig)
  }

  protected val converters = new GenericObjectPool[SimpleFeatureConverter[_]](factory)

  override def createLocalConverter(path: String, failures: AtomicLong): LocalIngestConverter =
    new LocalIngestConverterImpl(sft, path, converters, failures)

  override def runDistributedJob(statusCallback: StatusCallback): (Long, Long) = {
    val conf = configuration.flatMap(conf => Some(new Configuration(conf)))
    new ConverterIngestJob(dsParams, sft, converterConfig, inputs, libjarsFile, libjarsPaths, conf).run(statusCallback)
  }
}

class LocalIngestConverterImpl(sft: SimpleFeatureType, path: String, converters: ObjectPool[SimpleFeatureConverter[_]], failures: AtomicLong)
    extends LocalIngestConverter {

  class LocalIngestCounter extends DefaultCounter {
    // keep track of failure at a global level, keep line counts and success local
    override def incFailure(i: Long): Unit = failures.getAndAdd(i)
    override def getFailure: Long          = failures.get()
  }

  protected val converter: SimpleFeatureConverter[_] = converters.borrowObject()
  protected val ec: EvaluationContext = converter.createEvaluationContext(Map("inputFilePath" -> path), new LocalIngestCounter)

  override def convert(is: InputStream): (SimpleFeatureType, Iterator[SimpleFeature]) = (sft, converter.process(is, ec))
  override def close(): Unit = converters.returnObject(converter)
}

/**
 * Distributed job that uses converters to process input files
 *
 * @param sft simple feature type
 * @param converterConfig converter definition
 */
class ConverterIngestJob(dsParams: Map[String, String],
                         sft: SimpleFeatureType,
                         converterConfig: Config,
                         paths: Seq[String],
                         libjarsFile: String,
                         libjarsPaths: Iterator[() => Seq[File]],
                         configuration: Option[Configuration] = None)
    extends AbstractIngestJob(dsParams, sft.getTypeName, paths, libjarsFile, libjarsPaths, configuration) {

  import ConverterInputFormat.{Counters => ConvertCounters}
  import GeoMesaOutputFormat.{Counters => OutCounters}

  val failCounters =
    Seq((ConvertCounters.Group, ConvertCounters.Failed), (OutCounters.Group, OutCounters.Failed))

  override val inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]] = classOf[ConverterInputFormat]

  override def configureJob(job: Job): Unit = {
    super.configureJob(job)
    ConverterInputFormat.setConverterConfig(job, converterConfig.root().render(ConfigRenderOptions.concise()))
    ConverterInputFormat.setSft(job, sft)
  }

  override def written(job: Job): Long =
    job.getCounters.findCounter(OutCounters.Group, OutCounters.Written).getValue

  override def failed(job: Job): Long =
    failCounters.map(c => job.getCounters.findCounter(c._1, c._2).getValue).sum
}
