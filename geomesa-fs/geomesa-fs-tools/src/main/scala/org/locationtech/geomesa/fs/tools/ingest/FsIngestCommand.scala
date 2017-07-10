/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File
import java.util

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

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
      throw new ParameterException("Converter Config argument is required")
    }
    if (params.spec == null) {
      throw new ParameterException("SimpleFeatureType specification argument is required")
    }

    if (params.scheme == null) {
      throw new ParameterException("Partition Scheme argument is required")
    }

    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    val converterConfig = CLArgResolver.getConfig(params.config)

    val scheme = PartitionSchemeArgResolver.getArg(SchemeArgs(params.scheme, sft)) match {
      case Right(scheme) => scheme
      case Left(e)    => throw new ParameterException(e)
    }
    PartitionScheme.addToSft(sft, scheme)

    // Can use this to set things like compression and summary levels for parquet in the sft user data
    // to be picked up by the ingest job
    params.storageOpts.foreach { s =>
      try {
        val arr = s.split("=", 1)
        val (k, v) = (arr(0), arr(1))
        sft.getUserData.put(k,v)
      } catch {
        case e: Throwable =>
          throw new ParameterException(s"Unable to parse storage opt $s")
      }
    }

    val ingest =
      new ParquetConverterIngest(sft,
        connection,
        converterConfig,
        params.files,
        libjarsFile,
        libjarsPaths,
        params.threads,
        new Path(params.path),
        Option(params.tempDir).map(new Path(_)),
        Option(params.reducers))
    ingest.run()
  }

}

@Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
class FsIngestParams extends IngestParams with FsParams {
  @Parameter(names = Array("--temp-path"), description = "Path to temp dir for parquet ingest. " +
    "Note that this may be useful when using s3 since its slow as a sink", required = false)
  var tempDir: String = _

  @Parameter(names = Array("--num-reducers"), description = "Num reducers (required for distributed ingest)", required = false)
  var reducers: java.lang.Integer = _

  @Parameter(names = Array("--partition-scheme"), description = "PartitionScheme typesafe config string or file", required = true)
  var scheme: java.lang.String = _

  @Parameter(names = Array("--storage-opt"), variableArity = true, description = "Additional storage opts (k=v)", required = false)
  var storageOpts: java.util.List[java.lang.String] = new util.ArrayList[String]()
}
