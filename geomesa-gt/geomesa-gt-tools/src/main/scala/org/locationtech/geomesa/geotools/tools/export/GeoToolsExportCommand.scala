/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.export

import com.beust.jcommander.Parameters
import org.geotools.data.DataStore
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.{GeoToolsDataStoreParams, GeoToolsDistributedCommand}
import org.locationtech.geomesa.geotools.tools.export.GeoToolsExportCommand.GeoToolsExportParams
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams

class GeoToolsExportCommand extends ExportCommand[DataStore] with GeoToolsDistributedCommand {
  override val params: GeoToolsExportParams = new GeoToolsExportParams
}

object GeoToolsExportCommand {
  @Parameters(commandDescription = "Export features from a data store")
  class GeoToolsExportParams extends ExportParams with GeoToolsDataStoreParams with RequiredTypeNameParam
}
