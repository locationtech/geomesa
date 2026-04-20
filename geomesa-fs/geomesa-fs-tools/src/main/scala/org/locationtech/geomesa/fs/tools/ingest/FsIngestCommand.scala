/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.core.Metadata
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsDistributedCommand, FsParams, OptionalSchemeParams}
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.ParquetConverterJob
import org.locationtech.geomesa.fs.tools.ingest.FsIngestCommand.FsIngestParams
import org.locationtech.geomesa.jobs.Awaitable
import org.locationtech.geomesa.jobs.mapreduce.ConverterCombineInputFormat
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.IngestCommand.{IngestParams, Inputs}
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.{Command, TempPathParam}

import java.net.URI

class FsIngestCommand extends IngestCommand[FileSystemDataStore] with FsDistributedCommand {

  override val params = new FsIngestParams

  override protected def setBackendSpecificOptions(sft: SimpleFeatureType): Unit =
    FsCreateSchemaCommand.setOptions(sft, params)

  override protected def startIngest(
      mode: RunMode,
      ds: FileSystemDataStore,
      sft: SimpleFeatureType,
      converter: Config,
      inputs: Inputs): Awaitable = {
    mode match {
      case RunModes.Local =>
        super.startIngest(mode, ds, sft, converter, inputs)

      case RunModes.Distributed =>
        Command.user.info(s"Running ingestion in distributed ${if (params.combineInputs) "combine " else "" }mode")
        val reducers = Option(params.reducers).filter(_ > 0).getOrElse {
          throw new ParameterException("Please specify --num-reducers for distributed ingest")
        }
        val storage = ds.storage(sft.getTypeName)
        val tmpPath = Option(params.tempPath).map(new URI(_))
        val targetFileSize = storage.metadata.get(Metadata.TargetFileSize).map(_.toLong)

        tmpPath.foreach { tp =>
          if (storage.context.fs.exists(tp)) {
            Command.user.info(s"Deleting temp path $tp")
            //TODO
//            storage.context.fs.delete(tp, true)
          }
        }

        new ParquetConverterJob(
          connection, sft, converter, inputs.paths, libjarsFiles, libjarsPaths, reducers,
          new Path(storage.context.root), storage.metadata.schemes, tmpPath.map(new Path(_)), targetFileSize) {
          override def configureJob(job: Job): Unit = {
            super.configureJob(job)
            if (params.combineInputs) {
              job.setInputFormatClass(classOf[ConverterCombineInputFormat])
              Option(params.maxSplitSize).foreach(s => FileInputFormat.setMaxInputSplitSize(job, s.toLong))
            }
          }
        }

      case _ =>
        throw new UnsupportedOperationException(s"Missing implementation for mode $mode")
    }
  }
}

object FsIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class FsIngestParams extends IngestParams with FsParams with OptionalSchemeParams with TempPathParam {
    @Parameter(
      names = Array("--num-reducers"),
      description = "Num reducers (required for distributed ingest)",
      required = false)
    var reducers: java.lang.Integer = _
  }
}
