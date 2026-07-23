/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.tools.`export`

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.trino.datastore.TrinoDataStore
import org.locationtech.geomesa.trino.tools.TrinoDataStoreCommand.{TrinoDistributedCommand, TrinoParams}
import org.locationtech.geomesa.trino.tools.`export`.TrinoExportCommand.TrinoExportParams

class TrinoExportCommand extends ExportCommand[TrinoDataStore] with TrinoDistributedCommand {
  override val params = new TrinoExportParams
}

object TrinoExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class TrinoExportParams extends ExportParams with TrinoParams with RequiredTypeNameParam
}
