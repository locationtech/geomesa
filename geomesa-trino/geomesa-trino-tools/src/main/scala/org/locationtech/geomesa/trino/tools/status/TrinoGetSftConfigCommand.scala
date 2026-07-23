/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.status.{GetSftConfigCommand, GetSftConfigParams}
import org.locationtech.geomesa.trino.datastore.TrinoDataStore
import org.locationtech.geomesa.trino.tools.TrinoDataStoreCommand
import org.locationtech.geomesa.trino.tools.TrinoDataStoreCommand.TrinoParams
import org.locationtech.geomesa.trino.tools.status.TrinoGetSftConfigCommand.TrinoGetSftConfigParameters

class TrinoGetSftConfigCommand extends GetSftConfigCommand[TrinoDataStore] with TrinoDataStoreCommand {
  override val params = new TrinoGetSftConfigParameters
}

object TrinoGetSftConfigCommand {
  @Parameters(commandDescription = "Get the SimpleFeatureType definition of a schema")
  class TrinoGetSftConfigParameters extends TrinoParams with GetSftConfigParams with RequiredTypeNameParam
}
