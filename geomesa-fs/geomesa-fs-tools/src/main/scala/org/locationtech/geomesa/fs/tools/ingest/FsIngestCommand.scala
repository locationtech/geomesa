/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.conf.{PartitionSchemeArgResolver, SchemeArgs}
import org.locationtech.geomesa.fs.storage.common.{PartitionOpts, PartitionScheme}
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.{OrcConverterJob, ParquetConverterJob}
import org.locationtech.geomesa.fs.tools.ingest.FsIngestCommand.{FileSystemConverterIngest, FsIngestParams}
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.ParameterConverters.KeyValueConverter
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

// TODO we need multi threaded ingest for this
class FsIngestCommand extends IngestCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params = new FsIngestParams

  override val libjarsFile: String = "org/locationtech/geomesa/fs/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_FS_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[FileSystemDataStore])
  )

  override def execute(): Unit = {
    // validate arguments
    if (params.config == null) {
      throw new ParameterException("Converter config argument is required")
    }
    if (params.spec == null) {
      throw new ParameterException("SimpleFeatureType specification argument is required")
    }
    if (params.fmt == DataFormats.Shp) {
      // TODO
      throw new ParameterException("Shapefile ingest is not currently supported for FileDataStore")
    }

    super.execute()
  }

  override protected def createConverterIngest(sft: SimpleFeatureType, converterConfig: Config): Runnable = {
    val scheme = PartitionSchemeArgResolver.getArg(SchemeArgs(params.scheme, sft)) match {
      case Left(e) => throw new ParameterException(e)
      case Right(s) if s.isLeafStorage == params.leafStorage => s
      case Right(s) =>
        val opts = s.getOptions.updated(PartitionOpts.LeafStorage, params.leafStorage.toString).toMap
        PartitionScheme.apply(sft, s.name(), opts)
    }

    PartitionScheme.addToSft(sft, scheme)

    // Can use this to set things like compression and summary levels for parquet in the sft user data
    // to be picked up by the ingest job
    params.storageOpts.foreach { case (k, v) => sft.getUserData.put(k,v) }

    new FileSystemConverterIngest(sft,
        connection,
        converterConfig,
        params.files,
        Option(params.mode),
        params.encoding,
        libjarsFile,
        libjarsPaths,
        params.threads,
        Option(params.tempDir).map(new Path(_)),
        Option(params.reducers))
  }
}

object FsIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class FsIngestParams extends IngestParams with FsParams with TempDirParam {
    @Parameter(names = Array("--num-reducers"), description = "Num reducers (required for distributed ingest)", required = false)
    var reducers: java.lang.Integer = _

    @Parameter(names = Array("--partition-scheme"), description = "PartitionScheme typesafe config string or file", required = true)
    var scheme: java.lang.String = _

    @Parameter(names = Array("--leaf-storage"), description = "Use Leaf Storage for Partition Scheme", required = false, arity = 1)
    var leafStorage: java.lang.Boolean = true

    @Parameter(names = Array("--storage-opt"), variableArity = true, description = "Additional storage opts (k=v)", required = false, converter = classOf[KeyValueConverter])
    var storageOpts: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()
  }

  trait TempDirParam {
    @Parameter(names = Array("--temp-path"), description = "Path to temp dir for writing output. " +
        "Note that this may be useful when using s3 since it is slow as a sink", required = false)
    var tempDir: String = _
  }


  class FileSystemConverterIngest(sft: SimpleFeatureType,
                                  dsParams: Map[String, String],
                                  converterConfig: Config,
                                  inputs: Seq[String],
                                  mode: Option[RunMode],
                                  encoding: String,
                                  libjarsFile: String,
                                  libjarsPaths: Iterator[() => Seq[File]],
                                  numLocalThreads: Int,
                                  tempPath: Option[Path],
                                  reducers: Option[java.lang.Integer])
      extends ConverterIngest(sft, dsParams, converterConfig, inputs, mode, libjarsFile, libjarsPaths, numLocalThreads) {

    override def runDistributedJob(statusCallback: StatusCallback): (Long, Long) = {
      if (reducers.isEmpty) {
        throw new ParameterException("Must provide num-reducers argument for distributed ingest")
      }
      val job = if (encoding == ParquetFileSystemStorage.ParquetEncoding) {
        new ParquetConverterJob()
      } else if (encoding == OrcFileSystemStorage.OrcEncoding) {
        new OrcConverterJob()
      } else {
        throw new ParameterException(s"Ingestion is not supported for encoding $encoding")
      }
      job.run(dsParams, sft.getTypeName, converterConfig, inputs, tempPath, reducers.get,
        libjarsFile, libjarsPaths, statusCallback)
    }
  }
}
