/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{File, InputStream}
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.locationtech.geomesa.convert.Transformers.DefaultCounter
import org.locationtech.geomesa.convert.{SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.jobs.mapreduce.{ConverterInputFormat, GeoMesaOutputFormat}
import org.locationtech.geomesa.tools.Command
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
                      libjarsFile: String,
                      libjarsPaths: Iterator[() => Seq[File]],
                      numLocalThreads: Int)
    extends AbstractIngest(dsParams, sft.getTypeName, inputs, libjarsFile, libjarsPaths, numLocalThreads) {

  override def beforeRunTasks(): Unit = {
    // create schema for the feature prior to Ingest job
    Command.user.info(s"Creating schema ${sft.getTypeName}")
    ds.createSchema(sft)
  }

  val factory = new BasePooledObjectFactory[SimpleFeatureConverter[_]] {
    override def wrap(obj: SimpleFeatureConverter[_]) = new DefaultPooledObject[SimpleFeatureConverter[_]](obj)
    override def create(): SimpleFeatureConverter[_] = SimpleFeatureConverters.build(sft, converterConfig)
  }

  private val converterPool =
    new GenericObjectPool[SimpleFeatureConverter[_]](factory)

  override def createLocalConverter(file: File, failures: AtomicLong): LocalIngestConverter =
    new LocalIngestConverter {

      class LocalIngestCounter extends DefaultCounter {
        // keep track of failure at a global level, keep line counts and success local
        override def incFailure(i: Long): Unit = failures.getAndAdd(i)
        override def getFailure: Long          = failures.get()
      }

      val converter = converterPool.borrowObject()
      val ec = converter.createEvaluationContext(Map("inputFilePath" -> file.getAbsolutePath), new LocalIngestCounter)

      override def convert(is: InputStream): (SimpleFeatureType, Iterator[SimpleFeature]) = (sft, converter.process(is, ec))
      override def close(): Unit = {
        converterPool.returnObject(converter)
      }
    }

  override def runDistributedJob(statusCallback: (Float, Long, Long, Boolean) => Unit = (_, _, _, _) => Unit): (Long, Long) = {
    val job = new ConverterIngestJob(sft, converterConfig)
    job.run(dsParams, sft.getTypeName, inputs, libjarsFile, libjarsPaths, statusCallback)
  }
}

/**
 * Distributed job that uses converters to process input files
 *
 * @param sft simple feature type
 * @param converterConfig converter definition
 */
class ConverterIngestJob(sft: SimpleFeatureType, converterConfig: Config) extends AbstractIngestJob {

  import ConverterInputFormat.{Counters => ConvertCounters}
  import GeoMesaOutputFormat.{Counters => OutCounters}

  val failCounters =
    Seq((ConvertCounters.Group, ConvertCounters.Failed), (OutCounters.Group, OutCounters.Failed))

  override val inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]] = classOf[ConverterInputFormat]

  override def configureJob(job: Job): Unit = {
    ConverterInputFormat.setConverterConfig(job, converterConfig.root().render(ConfigRenderOptions.concise()))
    ConverterInputFormat.setSft(job, sft)
  }

  override def written(job: Job): Long =
    job.getCounters.findCounter(OutCounters.Group, OutCounters.Written).getValue

  override def failed(job: Job): Long =
    failCounters.map(c => job.getCounters.findCounter(c._1, c._2).getValue).sum
}
