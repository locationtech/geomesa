/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import java.net.URL
import java.util
import java.util.Locale

import com.beust.jcommander.{JCommander, Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.Utils.Formats
import org.locationtech.geomesa.tools.accumulo.Utils.Formats._
import org.locationtech.geomesa.tools.accumulo.commands.IngestCommand._
import org.locationtech.geomesa.tools.accumulo.ingest.{AutoIngest, ConverterIngest}
import org.locationtech.geomesa.tools.common.commands._
import org.locationtech.geomesa.tools.common.{CLArgResolver, OptionalFeatureTypeNameParam, OptionalFeatureTypeSpecParam}
import org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Try

class IngestCommand(parent: JCommander) extends Command(parent) with LazyLogging {
  override val command = "ingest"
  override val params = new IngestParameters()

  // If you change this, update the regex in GeneralShapefileIngest for URLs
  private val remotePrefixes = Seq("hdfs", "s3n", "s3a")

  def isDistributedUrl(url: String) = remotePrefixes.exists(url.startsWith)

  override def execute(): Unit = {
    ensureSameFs(remotePrefixes)

    val fmtParam = Option(params.format).flatMap(f => Try(Formats.withName(f.toLowerCase(Locale.US))).toOption)
    lazy val fmtFile = params.files.flatMap(f => Try(Formats.withName(getFileExtension(f))).toOption).headOption
    val fmt = fmtParam.orElse(fmtFile).getOrElse(Other)

    if (fmt == SHP) {
      val ds = params.createDataStore()

      // If someone is ingesting file from hdfs or S3, we add the Hadoop URL Factories to the JVM.
      if (params.files.exists(isDistributedUrl)) {
        import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
        val factory = new FsUrlStreamHandlerFactory
        URL.setURLStreamHandlerFactory(factory)
      }

      if (params.threads > 1) {
        val parfiles = params.files.par
        parfiles.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(params.threads))
        parfiles.foreach(GeneralShapefileIngest.shpToDataStore(_, ds, params.featureName))
      } else {
        params.files.foreach(GeneralShapefileIngest.shpToDataStore(_, ds, params.featureName))
      }
      ds.dispose()
    } else {
      val tryDs = DataStoreFinder.getDataStore(params.dataStoreParams)
      if (tryDs == null) {
        throw new ParameterException("Could not load a data store with the provided parameters")
      }
      tryDs.dispose()

      // if there is no sft and no converter passed in, try to use the auto ingest which will
      // pick up the schema from the input files themselves
      if (params.spec == null && params.config == null && Seq(TSV, CSV, AVRO).contains(fmt)) {
        if (params.featureName == null) {
          throw new ParameterException("Feature name is required when a schema is not specified")
        }
        // auto-detect the import schema
        logger.info("No schema or converter defined - will attempt to detect schema from input files")
        new AutoIngest(params.dataStoreParams, params.featureName, params.files, params.threads, fmt).run()
      } else {
        val sft = CLArgResolver.getSft(params.spec, params.featureName)
        val converterConfig = CLArgResolver.getConfig(params.config)
        new ConverterIngest(params.dataStoreParams, params.files, params.threads, sft, converterConfig).run()
      }
    }
  }

  def ensureSameFs(prefixes: Seq[String]): Unit =
    prefixes.foreach { pre =>
      if (params.files.exists(_.toLowerCase.startsWith(s"$pre://")) &&
        !params.files.forall(_.toLowerCase.startsWith(s"$pre://"))) {
        throw new ParameterException(s"Files must all be on the same file system: ($pre) or all be local")
      }
    }
}

object IngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class IngestParameters extends GeoMesaConnectionParams
    with OptionalFeatureTypeNameParam
    with OptionalFeatureTypeSpecParam {

    @Parameter(names = Array("-C", "--converter"), description = "GeoMesa converter specification as a config string, file name, or name of an available converter")
    var config: String = null

    @Parameter(names = Array("-F", "--format"), description = "File format of input files (shp, csv, tsv, avro, etc)")
    var format: String = null

    @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local ingest")
    var threads: Integer = 1

    @Parameter(description = "<file>...", required = true)
    var files: java.util.List[String] = new util.ArrayList[String]()
  }
}
