/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.status.AccumuloGetTypeNamesCommand.GetTypeNamesParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.status.GetTypeNamesCommand

class AccumuloGetTypeNamesCommand extends GetTypeNamesCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new GetTypeNamesParams()
}

object AccumuloGetTypeNamesCommand {
  @Parameters(commandDescription = "List the feature types for a given catalog")
  class GetTypeNamesParams extends AccumuloDataStoreParams
}
