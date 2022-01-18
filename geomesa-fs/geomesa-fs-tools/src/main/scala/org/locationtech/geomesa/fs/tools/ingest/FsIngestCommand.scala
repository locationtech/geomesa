/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.api.Metadata
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsDistributedCommand, FsParams, OptionalEncodingParam, OptionalSchemeParams}
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.{OrcConverterJob, ParquetConverterJob}
import org.locationtech.geomesa.fs.tools.ingest.FsIngestCommand.FsIngestParams
import org.locationtech.geomesa.jobs.Awaitable
import org.locationtech.geomesa.jobs.mapreduce.ConverterCombineInputFormat
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams
import org.locationtech.geomesa.tools.ingest._
import org.opengis.feature.simple.SimpleFeatureType

class FsIngestCommand extends IngestCommand[FileSystemDataStore] with FsDistributedCommand {

  override val params = new FsIngestParams

  override protected def setBackendSpecificOptions(sft: SimpleFeatureType): Unit =
    FsCreateSchemaCommand.setOptions(sft, params)

  override protected def startIngest(
      mode: RunMode,
      ds: FileSystemDataStore,
      sft: SimpleFeatureType,
      converter: Config,
      inputs: Seq[String]): Awaitable = {
    mode match {
      case RunModes.Local =>
        super.startIngest(mode, ds, sft, converter, inputs)

      case RunModes.Distributed =>
        Command.user.info(s"Running ingestion in distributed ${if (params.combineInputs) "combine " else "" }mode")
        val reducers = Option(params.reducers).filter(_ > 0).getOrElse {
          throw new ParameterException("Please specify --num-reducers for distributed ingest")
        }
        val storage = ds.storage(sft.getTypeName)
        val tmpPath = Option(params.tempDir).map(d => storage.context.fc.makeQualified(new Path(d)))
        val targetFileSize = storage.metadata.get(Metadata.TargetFileSize).map(_.toLong)

        tmpPath.foreach { tp =>
          if (storage.context.fc.util.exists(tp)) {
            Command.user.info(s"Deleting temp path $tp")
            storage.context.fc.delete(tp, true)
          }
        }

        storage.metadata.encoding match {
          case OrcFileSystemStorage.Encoding =>
            new OrcConverterJob(
              connection, sft, converter, inputs, libjarsFiles, libjarsPaths, reducers,
              storage.context.root, tmpPath, targetFileSize) {
              override def configureJob(job: Job): Unit = {
                super.configureJob(job)
                if (params.combineInputs) {
                  job.setInputFormatClass(classOf[ConverterCombineInputFormat])
                  Option(params.maxSplitSize).foreach(s => FileInputFormat.setMaxInputSplitSize(job, s.toLong))
                }
              }
            }

          case ParquetFileSystemStorage.Encoding =>
            new ParquetConverterJob(
              connection, sft, converter, inputs, libjarsFiles, libjarsPaths, reducers,
              storage.context.root, tmpPath, targetFileSize) {
              override def configureJob(job: Job): Unit = {
                super.configureJob(job)
                if (params.combineInputs) {
                  job.setInputFormatClass(classOf[ConverterCombineInputFormat])
                  Option(params.maxSplitSize).foreach(s => FileInputFormat.setMaxInputSplitSize(job, s.toLong))
                }
              }
            }

          case _ =>
            throw new ParameterException(s"Ingestion is not supported for encoding '${params.encoding}'")
        }

      case _ =>
        throw new NotImplementedError(s"Missing implementation for mode $mode")
    }
  }
}

object FsIngestCommand {

  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class FsIngestParams extends IngestParams with FsParams with OptionalEncodingParam with OptionalSchemeParams with TempDirParam {
    @Parameter(names = Array("--num-reducers"), description = "Num reducers (required for distributed ingest)", required = false)
    var reducers: java.lang.Integer = _
  }

  trait TempDirParam {
    @Parameter(names = Array("--temp-path"), description = "Path to temp dir for writing output. " +
        "Note that this may be useful when using s3 since it is slow as a sink", required = false)
    var tempDir: String = _
  }
}
