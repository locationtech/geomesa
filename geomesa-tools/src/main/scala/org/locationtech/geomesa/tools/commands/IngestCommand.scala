/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.commands

import java.util

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools.commands.IngestCommand._
import org.locationtech.geomesa.tools.ingest.ConverterIngest
import org.locationtech.geomesa.tools.{CLArgResolver, DataStoreHelper}
import org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest

import scala.collection.JavaConversions._

class IngestCommand(parent: JCommander) extends Command(parent) with LazyLogging {
  override val command = "ingest"
  override val params = new IngestParameters()

  override def execute(): Unit = {
    val fmt = Option(params.format).getOrElse(getFileExtension(params.files(0)))
    if (fmt == SHP) {
      val ds = new DataStoreHelper(params).getDataStore()
      GeneralShapefileIngest.shpToDataStore(params.files(0), ds, params.featureName)
    } else {
      ensureSameFs(Seq("hdfs", "s3n"))

      val dsParams = new DataStoreHelper(params).paramMap
      require(DataStoreFinder.getDataStore(dsParams) != null, "Could not load a data store with the provided parameters")
      val sft = CLArgResolver.getSft(params.spec, params.featureName)
      val converterConfig = CLArgResolver.getConfig(params.config)

      new ConverterIngest(dsParams, sft, converterConfig, params.files, params.threads).run()
    }
  }

  def ensureSameFs(prefixes: Seq[String]): Unit =
    prefixes.foreach { pre =>
      if (params.files.exists(_.toLowerCase.startsWith(s"$pre://")) &&
        !params.files.forall(_.toLowerCase.startsWith(s"$pre://"))) {
        throw new IllegalArgumentException(s"Files must all be on the same file system: ($pre) or all be local")
      }
    }

}

object IngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class IngestParameters extends OptionalFeatureParams {
    @Parameter(names = Array("-s", "--spec"), description = "SimpleFeatureType specification as a GeoTools spec, SFT config, or name of an available type")
    var spec: String = null

    @Parameter(names = Array("-C", "--converter"), description = "GeoMesa converter specification as a config string, file name, or name of an available converter")
    var config: String = null

    @Parameter(names = Array("-F", "--format"), description = "indicate non-converter ingest (shp)")
    var format: String = null

    @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local ingest")
    var threads: Integer = 1

    @Parameter(description = "<file>...", required = true)
    var files: java.util.List[String] = new util.ArrayList[String]()
  }
}
