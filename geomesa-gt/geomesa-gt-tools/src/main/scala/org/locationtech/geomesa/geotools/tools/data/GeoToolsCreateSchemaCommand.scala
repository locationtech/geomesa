/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.data

import com.beust.jcommander.Parameters
import org.geotools.data.DataStore
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.GeoToolsDataStoreParams
import org.locationtech.geomesa.geotools.tools.data.GeoToolsCreateSchemaCommand.GeoToolsCreateSchemaParams
import org.locationtech.geomesa.tools.data.CreateSchemaCommand
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.CreateSchemaParams

class GeoToolsCreateSchemaCommand extends CreateSchemaCommand[DataStore] with GeoToolsDataStoreCommand {
  override val params: GeoToolsCreateSchemaParams = new GeoToolsCreateSchemaParams()
}

object GeoToolsCreateSchemaCommand {
  @Parameters(commandDescription = "Create a new feature type in a data store")
  class GeoToolsCreateSchemaParams extends CreateSchemaParams with GeoToolsDataStoreParams
}
