/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.ingest

import java.io.File

import com.beust.jcommander.Parameters
import org.geotools.data.DataStore
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.GeoToolsDataStoreParams
import org.locationtech.geomesa.geotools.tools.ingest.GeoToolsIngestCommand.GeoToolsIngestParams
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

class GeoToolsIngestCommand extends IngestCommand[DataStore] with GeoToolsDataStoreCommand {

  override val params: GeoToolsIngestParams = new GeoToolsIngestParams()

  override val libjarsFile: String = "org/locationtech/geomesa/geotools/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_GEOTOOLS_HOME")
  )
}

object GeoToolsIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into a data store")
  class GeoToolsIngestParams extends IngestParams with GeoToolsDataStoreParams
}
