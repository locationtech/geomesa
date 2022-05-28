/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.ingest

import java.io.File

import com.beust.jcommander.Parameters
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.jobs.HBaseIndexFileMapper
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.HBaseDistributedCommand
import org.locationtech.geomesa.hbase.tools.ingest.HBaseBulkIngestCommand.HBaseBulkIngestParams
import org.locationtech.geomesa.hbase.tools.ingest.HBaseIngestCommand.HBaseIngestParams
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.mapreduce.ConverterCombineInputFormat
import org.locationtech.geomesa.jobs.{Awaitable, JobResult, StatusCallback}
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.IngestCommand.Inputs
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.{Command, OutputPathParam, RequiredIndexParam}
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

class HBaseBulkIngestCommand extends HBaseIngestCommand with HBaseDistributedCommand {

  override val name = "bulk-ingest"
  override val params = new HBaseBulkIngestParams()

  override protected def startIngest(
      mode: RunMode,
      ds: HBaseDataStore,
      sft: SimpleFeatureType,
      converter: Config,
      inputs: Inputs): Awaitable = {

    mode match {
      case RunModes.Local =>
        throw new IllegalArgumentException("Bulk ingest must be run in distributed mode")

      case RunModes.Distributed =>
        // validate index param now that we have a datastore and the sft has been created
        val index = params.loadIndex(ds, sft.getTypeName, IndexMode.Write).identifier
        Command.user.info(s"Running bulk ingestion in distributed ${if (params.combineInputs) "combine " else "" }mode")
        new BulkConverterIngest(connection, sft, converter, inputs.paths, libjarsFiles, libjarsPaths, index) {
          override def configureJob(job: Job): Unit = {
            super.configureJob(job)
            if (params.combineInputs) {
              job.setInputFormatClass(classOf[ConverterCombineInputFormat])
              Option(params.maxSplitSize).foreach(s => FileInputFormat.setMaxInputSplitSize(job, s.toLong))
            }
          }
        }

      case _ =>
        throw new NotImplementedError(s"Missing implementation for mode $mode")
    }
  }

  class BulkConverterIngest(
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      libjarsFiles: Seq[String],
      libjarsPaths: Iterator[() => Seq[File]],
      index: String
    ) extends ConverterIngestJob(dsParams, sft, converterConfig, paths, libjarsFiles, libjarsPaths) {

    override def configureJob(job: Job): Unit = {
      super.configureJob(job)
      HBaseIndexFileMapper.configure(job, dsParams, sft.getTypeName, index, new Path(params.outputPath))
    }

    override def await(reporter: StatusCallback): JobResult = {
      super.await(reporter).merge {
        val update =
          ". To load files, run:\n  geomesa-hbase bulk-load " +
              s"-c ${params.catalog} -f ${sft.getTypeName} --index ${params.index} --input ${params.outputPath}"
        Some(JobSuccess(update, Map.empty))
      }
    }
  }
}

object HBaseBulkIngestCommand {
  @Parameters(commandDescription = "Convert various file formats into HBase HFiles suitable for incremental load")
  class HBaseBulkIngestParams extends HBaseIngestParams with RequiredIndexParam with OutputPathParam
}
