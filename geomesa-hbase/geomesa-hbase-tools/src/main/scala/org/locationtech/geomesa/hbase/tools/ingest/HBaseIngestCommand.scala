/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.ingest

import java.io.File

import com.beust.jcommander.Parameters
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.Connection
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.hbase.tools.ingest.HBaseIngestCommand.HBaseIngestParams
import org.locationtech.geomesa.tools.ingest.{ConverterIngest, IngestCommand, IngestParams}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class HBaseIngestCommand extends IngestCommand[HBaseDataStore] with HBaseDataStoreCommand {

  override val params = new HBaseIngestParams()

  // TODO need to pass hbase-site.xml around?
  override val libjarsFile: String = "org/locationtech/geomesa/hbase/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HBASE_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("HBASE_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[HBaseDataStore]),
    () => ClassPathUtils.getJarsFromClasspath(classOf[Connection])
  )

  override protected def createConverterIngest(sft: SimpleFeatureType, converterConfig: Config): Runnable = {
    val configuration = withDataStore(hds => hds.connection.getConfiguration)
    new ConverterIngest(sft, connection, converterConfig, params.files, Option(params.mode),
      libjarsFile, libjarsPaths, params.threads, Some(configuration))
  }
}

object HBaseIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class HBaseIngestParams extends IngestParams with HBaseParams with ToggleRemoteFilterParam
}
