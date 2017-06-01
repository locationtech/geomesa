/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException}
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats}

trait IngestCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  import scala.collection.JavaConversions._

  override val name = "ingest"
  override def params: IngestParams

  def libjarsFile: String
  def libjarsPaths: Iterator[() => Seq[File]]

  override def execute(): Unit = {
    import DataFormats.{Avro, Csv, Shp, Tsv}

    ensureSameFs(IngestCommand.RemotePrefixes)

    val ingest = if (params.fmt == Shp) {
      new ShapefileIngest(connection, Option(params.featureName), params.files, params.threads)
    } else if (params.spec == null && params.config == null && Seq(Csv, Tsv, Avro).contains(params.fmt)) {
      // if there is no sft and no converter passed in, try to use the auto ingest which will
      // pick up the schema from the input files themselves
      if (params.featureName == null) {
        throw new ParameterException("Feature name is required when a schema is not specified")
      }
      // auto-detect the import schema
      Command.user.info("No schema or converter defined - will attempt to detect schema from input files")
      new AutoIngest(params.featureName, connection, params.files, params.fmt, libjarsFile, libjarsPaths, params.threads)
    } else {
      // validate arguments
      if (params.config == null) {
        throw new ParameterException("Converter Config argument is required")
      }
      if (params.spec == null) {
        throw new ParameterException("SimpleFeatureType specification argument is required")
      }

      val sft = CLArgResolver.getSft(params.spec, params.featureName)
      val converterConfig = CLArgResolver.getConfig(params.config)
      new ConverterIngest(sft, connection, converterConfig, params.files, libjarsFile, libjarsPaths, params.threads)
    }

    ingest.run()
  }

  def ensureSameFs(prefixes: Seq[String]): Unit = {
    prefixes.foreach { pre =>
      if (params.files.exists(_.toLowerCase.startsWith(s"$pre://")) &&
        !params.files.forall(_.toLowerCase.startsWith(s"$pre://"))) {
        throw new ParameterException(s"Files must all be on the same file system: ($pre) or all be local")
      }
    }
  }
}

// @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
trait IngestParams extends OptionalTypeNameParam with OptionalFeatureSpecParam
  with OptionalConverterConfigParam with OptionalInputFormatParam {
  @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local ingest")
  var threads: Integer = 1
}

object IngestCommand {
  // If you change this, update the regex in GeneralShapefileIngest for URLs
  private val RemotePrefixes = Seq("hdfs", "s3n", "s3a", "wasb", "wasbs")

  def isDistributedUrl(url: String): Boolean = RemotePrefixes.exists(url.startsWith)
}
