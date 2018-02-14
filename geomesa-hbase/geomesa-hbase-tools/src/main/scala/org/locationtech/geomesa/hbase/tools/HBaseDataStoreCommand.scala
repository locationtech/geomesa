/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseDataStoreParams}
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.RemoteFilterParam
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand}

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait HBaseDataStoreCommand extends DataStoreCommand[HBaseDataStore] {

  override def params: CatalogParam with RemoteFilterParam

  override def connection: Map[String, String] = {
    Map(
      HBaseDataStoreParams.HBaseCatalogParam.getName    -> params.catalog,
      HBaseDataStoreParams.RemoteFilteringParam.getName -> (!params.noRemote).toString
    )
  }
}

object HBaseDataStoreCommand {

  /**
    * Disables remote filtering/coprocessors
    */
  trait RemoteFilterParam {
    def noRemote: Boolean
  }

  /**
    * Exposes remote filtering as a command argument
    */
  trait ToggleRemoteFilterParam extends RemoteFilterParam {
    @Parameter(names = Array("--no-remote-filters"), description = "Disable remote filtering and coprocessors", arity = 0)
    var noRemote: Boolean = false
  }

  /**
    * Doesn't expose filtering as a command argument, for commands that wouldn't use filtering anyway.
    * Note that this means remote filtering is enabled.
    */
  trait RemoteFilterNotUsedParam extends RemoteFilterParam {
    override def noRemote: Boolean = false
  }
}
