/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.tools

import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.DataStoreCommand

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait AccumuloDataStoreCommand extends DataStoreCommand[AccumuloDataStore] {

  override def params: AccumuloDataStoreParams

  override def connection: Map[String, String] = Map[String, String](
    AccumuloDataStoreParams.instanceIdParam.getName -> params.instance,
    AccumuloDataStoreParams.zookeepersParam.getName -> params.zookeepers,
    AccumuloDataStoreParams.userParam.getName       -> params.user,
    AccumuloDataStoreParams.passwordParam.getName   -> params.password,
    AccumuloDataStoreParams.tableNameParam.getName  -> params.catalog,
    AccumuloDataStoreParams.visibilityParam.getName -> params.visibilities,
    AccumuloDataStoreParams.authsParam.getName      -> params.auths,
    AccumuloDataStoreParams.mockParam.getName       -> params.mock.toString
  ).filter(_._2 != null)
}
