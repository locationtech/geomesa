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
import org.locationtech.geomesa.geotools.tools.data.GeoToolsUpdateSchemaCommand.GeoToolsUpdateSchemaParams
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand.UpdateSchemaParams

class GeoToolsUpdateSchemaCommand extends UpdateSchemaCommand[DataStore] with GeoToolsDataStoreCommand {
  override val params = new GeoToolsUpdateSchemaParams()
}

object GeoToolsUpdateSchemaCommand {
  @Parameters(commandDescription = "Update a feature type")
  class GeoToolsUpdateSchemaParams extends UpdateSchemaParams with GeoToolsDataStoreParams
}
