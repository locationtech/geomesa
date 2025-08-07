/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools.commands

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.tools.CassandraDataStoreCommand
import org.locationtech.geomesa.cassandra.tools.CassandraDataStoreCommand.CassandraDataStoreParams
import org.locationtech.geomesa.cassandra.tools.commands.CassandraGetTypeNamesCommand.GetTypeNamesParams
import org.locationtech.geomesa.tools.status.GetTypeNamesCommand

class CassandraGetTypeNamesCommand extends GetTypeNamesCommand[CassandraDataStore] with CassandraDataStoreCommand {
  override val params = new GetTypeNamesParams
}

object CassandraGetTypeNamesCommand {
  @Parameters(commandDescription = "List the feature types for a given catalog")
  class GetTypeNamesParams extends CassandraDataStoreParams
}
