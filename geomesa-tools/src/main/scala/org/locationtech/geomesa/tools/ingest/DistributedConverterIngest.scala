/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.File

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.geotools.data.DataStore
import org.locationtech.geomesa.jobs.mapreduce.{ConverterInputFormat, GeoMesaOutputFormat}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractConverterIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.DistributedConverterIngest.ConverterIngestJob
import org.locationtech.geomesa.utils.text.TextTools
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Ingestion that uses geomesa converters to process input files
  *
  * @param dsParams data store parameters
  * @param sft simple feature type
  * @param converterConfig converter definition
  * @param inputs files to ingest
  * @param libjarsFile file with list of jars needed for ingest
  * @param libjarsPaths paths to search for libjars
  * @param waitForCompletion wait for the job to complete before returning
  */
class DistributedConverterIngest(
    dsParams: Map[String, String],
    sft: SimpleFeatureType,
    converterConfig: Config,
    inputs: Seq[String],
    libjarsFile: String,
    libjarsPaths: Iterator[() => Seq[File]],
    waitForCompletion: Boolean = true
  ) extends AbstractConverterIngest(dsParams, sft) {

  override protected def runIngest(ds: DataStore, sft: SimpleFeatureType, callback: StatusCallback): Unit = {
    Command.user.info("Running ingestion in distributed mode")
    val start = System.currentTimeMillis()
    createJob().run(callback, waitForCompletion).foreach { case (success, failed) =>
      Command.user.info(s"Distributed ingestion complete in ${TextTools.getTime(start)}")
      Command.user.info(IngestCommand.getStatInfo(success, failed))
    }
  }

  protected def createJob(): ConverterIngestJob =
    new ConverterIngestJob(dsParams, sft, converterConfig, inputs, libjarsFile, libjarsPaths)
}

object DistributedConverterIngest {

  /**
    * Distributed job that uses converters to process input files
    *
    * @param sft simple feature type
    * @param converterConfig converter definition
    */
  class ConverterIngestJob(
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      libjarsFile: String,
      libjarsPaths: Iterator[() => Seq[File]]
    ) extends AbstractIngestJob(dsParams, sft.getTypeName, paths, libjarsFile, libjarsPaths) {

    import ConverterInputFormat.{Counters => ConvertCounters}
    import GeoMesaOutputFormat.{Counters => OutCounters}

    private val failCounters =
      Seq((ConvertCounters.Group, ConvertCounters.Failed), (OutCounters.Group, OutCounters.Failed))

    override val inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]] = classOf[ConverterInputFormat]

    override def configureJob(job: Job): Unit = {
      super.configureJob(job)
      ConverterInputFormat.setConverterConfig(job, converterConfig.root().render(ConfigRenderOptions.concise()))
      ConverterInputFormat.setSft(job, sft)
    }

    override def written(job: Job): Long = job.getCounters.findCounter(OutCounters.Group, OutCounters.Written).getValue

    override def failed(job: Job): Long = failCounters.map(c => job.getCounters.findCounter(c._1, c._2).getValue).sum
  }
}
