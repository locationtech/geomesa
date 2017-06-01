/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools.commands

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.tools.{CassandraConnectionParams, CassandraDataStoreCommand, CassandraDataStoreParams}
import org.locationtech.geomesa.tools.status.DescribeSchemaCommand
import org.locationtech.geomesa.tools.{CatalogParam, RequiredTypeNameParam}


class CassandraDescribeSchemaCommand extends DescribeSchemaCommand[CassandraDataStore] with CassandraDataStoreCommand {
  override val params = new CassandraDescribeSchemaParams
}

@Parameters(commandDescription = "Describe the attributes of a given GeoMesa feature type")
class CassandraDescribeSchemaParams extends CassandraDataStoreParams with RequiredTypeNameParam
