/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.tools

import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseDataStoreFactory}
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait HBaseDataStoreCommand extends DataStoreCommand[HBaseDataStore] {

  override def params: CatalogParam

  override def connection: Map[String, String] = {
    val remote = GeoMesaSystemProperties.SystemProperty("geomesa.hbase.remote.filtering", "false").get.toBoolean
    Map(
      HBaseDataStoreFactory.Params.BigTableNameParam.getName -> params.catalog,
      HBaseDataStoreFactory.Params.RemoteParam.getName       -> remote.toString
    )
  }
}
