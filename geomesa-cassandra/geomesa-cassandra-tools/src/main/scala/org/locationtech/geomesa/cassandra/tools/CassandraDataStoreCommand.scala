/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools

import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraDataStoreFactory}
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand}

trait CassandraDataStoreCommand extends DataStoreCommand[CassandraDataStore] {

  override def params: CassandraConnectionParams with CatalogParam

  override def connection: Map[String, String] = Map(
    CassandraDataStoreFactory.Params.CatalogParam.getName      -> params.catalog,
    CassandraDataStoreFactory.Params.KeySpaceParam.getName     -> params.keySpace,
    CassandraDataStoreFactory.Params.ContactPointParam.getName -> params.contactPoint,
    CassandraDataStoreFactory.Params.UserNameParam.getName     -> params.user,
    CassandraDataStoreFactory.Params.PasswordParam.getName     -> params.password
  )
}

object CassandraDataStoreCommand {
  trait CassandraDataStoreParams extends CassandraConnectionParams with CatalogParam
}
