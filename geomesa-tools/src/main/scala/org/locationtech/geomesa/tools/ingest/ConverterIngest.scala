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
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool, PooledObject}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.geotools.data.DataUtilities.compare
import org.locationtech.geomesa.convert.{DefaultCounter, EvaluationContext}
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.jobs.mapreduce.{ConverterCombineInputFormat, ConverterInputFormat, GeoMesaOutputFormat}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.AbstractIngest.{LocalIngestConverter, StatusCallback}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
  * @param maxSplitSize max size of maps in a distributed ingest
  * @param waitForCompletion wait for the job to complete before returning
  */
class ConverterIngest(sft: SimpleFeatureType,
                      dsParams: Map[String, String],
                      converterConfig: Config,
                      inputs: Seq[String],
                      mode: Option[RunMode],
                      libjarsFile: String,
                      libjarsPaths: Iterator[() => Seq[File]],
                      numLocalThreads: Int,
                      maxSplitSize: Option[Integer] = None,
                      waitForCompletion: Boolean = true)
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

  private val factory = new BasePooledObjectFactory[SimpleFeatureConverter] {
    override def wrap(obj: SimpleFeatureConverter) = new DefaultPooledObject[SimpleFeatureConverter](obj)
    override def create(): SimpleFeatureConverter = SimpleFeatureConverter(sft, converterConfig)
    override def destroyObject(p: PooledObject[SimpleFeatureConverter]): Unit = p.getObject.close()
  }

  protected val converters = new GenericObjectPool[SimpleFeatureConverter](factory)

  override def run(): Unit = {
    try { super.run() } finally {
      converters.close()
    }
  }

  override def runDistributed(wait: Boolean): Unit = {
    super.runDistributed(waitForCompletion)
  }

  override def createLocalConverter(path: String, failures: AtomicLong): LocalIngestConverter =
    new LocalIngestConverterImpl(sft, path, converters, failures)

  override def runDistributedJob(statusCallback: StatusCallback, waitForCompletion: Boolean): Option[(Long, Long)] = {
    // Check conf if we should run against small files and use Combine* classes accordingly
    if (mode.contains(RunModes.DistributedCombine)) {
      new ConverterCombineIngestJob(dsParams, sft, converterConfig, inputs, maxSplitSize, libjarsFile, libjarsPaths).run(statusCallback, waitForCompletion)
    } else {
      new ConverterIngestJob(dsParams, sft, converterConfig, inputs, libjarsFile, libjarsPaths).run(statusCallback, waitForCompletion)
    }
  }
}

class LocalIngestConverterImpl(sft: SimpleFeatureType,
                               path: String,
                               converters: ObjectPool[SimpleFeatureConverter],
                               failures: AtomicLong) extends LocalIngestConverter {

  class LocalIngestCounter extends DefaultCounter {
    // keep track of failure at a global level, keep line counts and success local
    override def incFailure(i: Long): Unit = failures.getAndAdd(i)
    override def getFailure: Long          = failures.get()
  }

  protected val converter: SimpleFeatureConverter = converters.borrowObject()
  protected val ec: EvaluationContext =
    converter.createEvaluationContext(EvaluationContext.inputFileParam(path), counter = new LocalIngestCounter)

  override def convert(is: InputStream): Iterator[SimpleFeature] = converter.process(is, ec)
  override def close(): Unit = converters.returnObject(converter)
}

abstract class AbstractConverterIngestJob(dsParams: Map[String, String],
                                          sft: SimpleFeatureType,
                                          converterConfig: Config,
                                          paths: Seq[String],
                                          libjarsFile: String,
                                          libjarsPaths: Iterator[() => Seq[File]])
  extends AbstractIngestJob(dsParams, sft.getTypeName, paths, libjarsFile, libjarsPaths) {

  import ConverterInputFormat.{Counters => ConvertCounters}
  import GeoMesaOutputFormat.{Counters => OutCounters}

  val failCounters =
    Seq((ConvertCounters.Group, ConvertCounters.Failed), (OutCounters.Group, OutCounters.Failed))

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
                         libjarsPaths: Iterator[() => Seq[File]])
  extends AbstractConverterIngestJob(dsParams, sft, converterConfig, paths, libjarsFile, libjarsPaths) {

  override val inputFormatClass: Class[ConverterInputFormat] = classOf[ConverterInputFormat]
}

/**
 * Distributed job that uses converters to process input files in batches. This
 * allows multiple files to be processed by one mapper. Batch size is controlled
 * by the 'maxSplitSize' and should be scaled with mapper memory.
 *
 * @param sft simple feature type
 * @param converterConfig converter definition
 * @param maxSplitSize size in bytes for each map split 
 */
class ConverterCombineIngestJob(dsParams: Map[String, String],
                                sft: SimpleFeatureType,
                                converterConfig: Config,
                                paths: Seq[String],
                                maxSplitSize: Option[Integer],
                                libjarsFile: String,
                                libjarsPaths: Iterator[() => Seq[File]])
  extends AbstractConverterIngestJob(dsParams, sft, converterConfig, paths, libjarsFile, libjarsPaths) {
  override val inputFormatClass: Class[ConverterCombineInputFormat] = classOf[ConverterCombineInputFormat]

  override def configureJob(job: Job): Unit = {
    super.configureJob(job)
    maxSplitSize.foreach(s => FileInputFormat.setMaxInputSplitSize(job, s.toLong))
  }
}
