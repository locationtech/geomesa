/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.tools.commands

import java.io.File

import com.beust.jcommander.{ParameterException, Parameters}
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.tools.{CassandraDataStoreCommand, CassandraDataStoreParams}
import org.locationtech.geomesa.tools.data.{DeleteFeaturesCommand, DeleteFeaturesParams}

class CassandraDeleteFeaturesCommand extends DeleteFeaturesCommand[CassandraDataStore] with CassandraDataStoreCommand {
  override val params = new CassandraDeleteFeaturesParams
}

@Parameters(commandDescription = "Delete features from a table in GeoMesa. Does not delete any tables or schema information.")
trait CassandraDeleteFeaturesParams extends DeleteFeaturesParams with CassandraDataStoreParams

