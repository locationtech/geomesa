/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.ingest

import com.beust.jcommander.Parameters
import org.geotools.data.DataStore
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.{GeoToolsDataStoreParams, GeoToolsDistributedCommand}
import org.locationtech.geomesa.geotools.tools.ingest.GeoToolsIngestCommand.GeoToolsIngestParams
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams

class GeoToolsIngestCommand extends IngestCommand[DataStore] with GeoToolsDistributedCommand {
  override val params: GeoToolsIngestParams = new GeoToolsIngestParams()
}

object GeoToolsIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into a data store")
  class GeoToolsIngestParams extends IngestParams with GeoToolsDataStoreParams
}
