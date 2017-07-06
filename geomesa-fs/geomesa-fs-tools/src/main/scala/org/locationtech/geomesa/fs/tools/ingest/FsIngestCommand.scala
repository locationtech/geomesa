/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import scala.collection.JavaConversions._
import scala.util.Try

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

    val scheme: org.locationtech.geomesa.fs.storage.api.PartitionScheme =
      Try(PartitionScheme.extractFromSft(sft)).toOption.orElse({
      val f = new java.io.File(params.scheme)
      if (f.exists()) {
        val conf = ConfigFactory.parseFile(f)
        Option(PartitionScheme(sft, conf))
      } else None
    }).orElse({
      val conf = ConfigFactory.parseString(params.scheme)
      Option(PartitionScheme(sft, conf))
    }).getOrElse(throw new ParameterException("Partition Scheme argument is required"))
    PartitionScheme.addToSft(sft, scheme)

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
        params.reducers)
    ingest.run()
  }

}

@Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
class FsIngestParams extends IngestParams with FsParams {
  @Parameter(names = Array("--temp-path"), description = "Path to temp dir for parquet ingest. " +
    "Note that this may be useful when using s3 since its slow as a sink", required = false)
  var tempDir: String = _

  @Parameter(names = Array("--num-reducers"), description = "Num reducers", required = true)
  var reducers: java.lang.Integer = _

  @Parameter(names = Array("--partition-scheme"), description = "PartitionScheme typesafe config string or file", required = true)
  var scheme: java.lang.String = _
}
