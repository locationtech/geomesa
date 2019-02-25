/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, OptionalEncodingParam, OptionalSchemeParams}
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.{OrcConverterJob, ParquetConverterJob}
import org.locationtech.geomesa.fs.tools.ingest.FsIngestCommand.FsIngestParams
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.DistributedConverterIngest.ConverterIngestJob
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.feature.simple.SimpleFeatureType

// TODO we need multi threaded ingest for this
class FsIngestCommand extends IngestCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params = new FsIngestParams

  override val libjarsFile: String = "org/locationtech/geomesa/fs/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_FS_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[FileSystemDataStore])
  )

  override protected def createIngest(
      mode: RunMode,
      sft: SimpleFeatureType,
      converter: Config,
      inputs: Seq[String]): Runnable = {
    FsCreateSchemaCommand.setOptions(sft, params)
    mode match {
      case RunModes.Local =>
        super.createIngest(mode, sft, converter, inputs)

      case RunModes.Distributed =>
        val reducers = Option(params.reducers).filter(_ > 0).getOrElse {
          throw new ParameterException("Please specify --num-reducers for distributed ingest")
        }
        val tmpPath = Option(params.tempDir).map(new Path(_))
        val wait = params.waitForCompletion
        val newJob = params.encoding match {
          case OrcFileSystemStorage.Encoding =>
            () => new OrcConverterJob(connection, sft, converter, inputs, libjarsFile, libjarsPaths, reducers, tmpPath)
          case ParquetFileSystemStorage.Encoding =>
            () => new ParquetConverterJob(connection, sft, converter, inputs, libjarsFile, libjarsPaths, reducers, tmpPath)
          case _ => throw new ParameterException(s"Ingestion is not supported for encoding '${params.encoding}'")
        }
        new DistributedConverterIngest(connection, sft, converter, inputs, libjarsFile, libjarsPaths, wait) {
          override protected def createJob(): ConverterIngestJob = newJob()
        }

      case RunModes.DistributedCombine =>
        throw new NotImplementedError("Distributed combine ingest is not supported for the FileSystem data store")

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
