/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.cassandra.tools

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraDataStoreParams}
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand}


trait CassandraDataStoreCommand extends DataStoreCommand[CassandraDataStore] {

  override val params = new CassandraDataStoreParams

  override def connection: Map[String, String] =
    Map(CassandraDataStoreParams.CATALOG.getName -> params.catalog,
      CassandraDataStoreParams.KEYSPACE.getName -> params.keySpace,
      CassandraDataStoreParams.NAMESPACE.getName -> params.nameSpace,
      CassandraDataStoreParams.CONTACT_POINT.getName -> params.contactPoint
      )
}

@Parameters(commandDescription = "Describe the attributes of a given GeoMesa feature type")
class CassandraDataStoreParams extends CassandraConnectionParams with CatalogParam
