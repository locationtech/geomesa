/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.status.GetTypeNamesCommand
import org.locationtech.geomesa.trino.datastore.TrinoDataStore
import org.locationtech.geomesa.trino.tools.TrinoDataStoreCommand
import org.locationtech.geomesa.trino.tools.TrinoDataStoreCommand.TrinoParams
import org.locationtech.geomesa.trino.tools.status.TrinoGetTypeNamesCommand.TrinoGetTypeNamesParams

class TrinoGetTypeNamesCommand extends GetTypeNamesCommand[TrinoDataStore] with TrinoDataStoreCommand {
  override val params = new TrinoGetTypeNamesParams()
}

object TrinoGetTypeNamesCommand {
  @Parameters(commandDescription = "List the feature types for a given schema")
  class TrinoGetTypeNamesParams extends TrinoParams
}
