/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.File

import com.typesafe.config.Config
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.geotools.data.DataStore
import org.locationtech.geomesa.jobs.mapreduce.ConverterCombineInputFormat
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractConverterIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.DistributedCombineConverterIngest.ConverterCombineIngestJob
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
  * @param maxSplitSize max size for input splits
  * @param waitForCompletion wait for the job to complete before returning
  */
class DistributedCombineConverterIngest(
    dsParams: Map[String, String],
    sft: SimpleFeatureType,
    converterConfig: Config,
    inputs: Seq[String],
    libjarsFile: String,
    libjarsPaths: Iterator[() => Seq[File]],
    maxSplitSize: Option[Integer] = None,
    waitForCompletion: Boolean = true
  ) extends AbstractConverterIngest(dsParams, sft) {

  override protected def runIngest(ds: DataStore, sft: SimpleFeatureType, callback: StatusCallback): Unit = {
    Command.user.info("Running ingestion in distributed combine mode")
    val start = System.currentTimeMillis()
    createJob().run(callback, waitForCompletion).foreach { case (success, failed) =>
      Command.user.info(s"Distributed ingestion complete in ${TextTools.getTime(start)}")
      Command.user.info(IngestCommand.getStatInfo(success, failed))
    }
  }

  protected def createJob(): ConverterCombineIngestJob =
    new ConverterCombineIngestJob(dsParams, sft, converterConfig, inputs, maxSplitSize, libjarsFile, libjarsPaths)
}

object DistributedCombineConverterIngest {

  /**
    * Distributed job that uses converters to process input files in batches. This
    * allows multiple files to be processed by one mapper. Batch size is controlled
    * by the 'maxSplitSize' and should be scaled with mapper memory.
    *
    * @param sft simple feature type
    * @param converterConfig converter definition
    * @param maxSplitSize size in bytes for each map split
    */
  class ConverterCombineIngestJob(
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      maxSplitSize: Option[Integer],
      libjarsFile: String,
      libjarsPaths: Iterator[() => Seq[File]]
    ) extends ConverterIngestJob(dsParams, sft, converterConfig, paths, libjarsFile, libjarsPaths) {

    override val inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]] = classOf[ConverterCombineInputFormat]

    override def configureJob(job: Job): Unit = {
      super.configureJob(job)
      maxSplitSize.foreach(s => FileInputFormat.setMaxInputSplitSize(job, s.toLong))
    }
  }
}
