/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools.commands

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.tools.CassandraDataStoreCommand
import org.locationtech.geomesa.cassandra.tools.CassandraDataStoreCommand.CassandraDataStoreParams
import org.locationtech.geomesa.cassandra.tools.commands.CassandraExplainCommand.CassandraExplainParams
import org.locationtech.geomesa.tools.status.{ExplainCommand, ExplainParams}

class CassandraExplainCommand extends ExplainCommand[CassandraDataStore] with CassandraDataStoreCommand {
  override val params = new CassandraExplainParams()
}

object CassandraExplainCommand {
  @Parameters(commandDescription = "Explain how a GeoMesa query will be executed")
  class CassandraExplainParams extends ExplainParams with CassandraDataStoreParams
}
