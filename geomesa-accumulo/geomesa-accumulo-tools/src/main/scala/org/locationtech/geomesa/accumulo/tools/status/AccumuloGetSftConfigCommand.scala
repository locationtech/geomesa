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
import org.locationtech.geomesa.accumulo.tools.status.AccumuloGetSftConfigCommand.AccumuloGetSftConfigParameters
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.status.{GetSftConfigCommand, GetSftConfigParams}

class AccumuloGetSftConfigCommand extends GetSftConfigCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloGetSftConfigParameters
}

object AccumuloGetSftConfigCommand {
  @Parameters(commandDescription = "Get the SimpleFeatureType definition of a schema")
  class AccumuloGetSftConfigParameters extends AccumuloDataStoreParams with GetSftConfigParams with RequiredTypeNameParam
}

