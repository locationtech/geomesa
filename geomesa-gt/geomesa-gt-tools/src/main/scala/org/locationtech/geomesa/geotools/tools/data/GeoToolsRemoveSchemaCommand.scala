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
import org.locationtech.geomesa.geotools.tools.data.GeoToolsRemoveSchemaCommand.GeoToolsRemoveSchemaParams
import org.locationtech.geomesa.tools.data.{RemoveSchemaCommand, RemoveSchemaParams}

class GeoToolsRemoveSchemaCommand extends RemoveSchemaCommand[DataStore] with GeoToolsDataStoreCommand {
  override val params: GeoToolsRemoveSchemaParams = new GeoToolsRemoveSchemaParams
}

object GeoToolsRemoveSchemaCommand {
  @Parameters(commandDescription = "Remove a schema and all associated features")
  class GeoToolsRemoveSchemaParams extends RemoveSchemaParams with GeoToolsDataStoreParams
}
