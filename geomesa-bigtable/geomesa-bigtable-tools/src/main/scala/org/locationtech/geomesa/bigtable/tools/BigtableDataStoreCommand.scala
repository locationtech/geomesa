/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.tools

import org.locationtech.geomesa.bigtable.data.BigtableDataStoreFactory
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand

trait BigtableDataStoreCommand extends HBaseDataStoreCommand {

  override def connection: Map[String, String] = {
    Map(
      BigtableDataStoreFactory.BigtableCatalogParam.getName -> params.catalog,
      HBaseDataStoreParams.ZookeeperParam.getName           -> params.zookeepers,
      HBaseDataStoreParams.RemoteFilteringParam.getName     -> "false",
      HBaseDataStoreParams.EnableSecurityParam.getName      -> params.secure.toString,
      HBaseDataStoreParams.AuthsParam.getName               -> params.auths
    ).filter(_._2 != null)
  }
}
