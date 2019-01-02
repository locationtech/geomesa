/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.kudu.data.{KuduDataStore, KuduDataStoreFactory}
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, PasswordParams}

/**
 * Abstract class for Kudu commands
 */
trait KuduDataStoreCommand extends DataStoreCommand[KuduDataStore] {

  override def params: KuduParams

  override def connection: Map[String, String] = {
    Map(
      KuduDataStoreFactory.Params.CatalogParam.getName       -> params.catalog,
      KuduDataStoreFactory.Params.KuduMasterParam.getName    -> params.master,
      KuduDataStoreFactory.Params.CredentialsParam.getName   -> params.password,
      KuduDataStoreFactory.Params.BossThreadsParam.getName   -> Option(params.bosses).map(_.toString).orNull,
      KuduDataStoreFactory.Params.WorkerThreadsParam.getName -> Option(params.workers).map(_.toString).orNull,
      KuduDataStoreFactory.Params.StatisticsParam.getName    -> Option(params.statistics).map(_.toString).orNull
    ).filter(_._2 != null)
  }
}

object KuduDataStoreCommand {

  trait KuduParams extends CatalogParam with PasswordParams {
    @Parameter(names = Array("-M", "--master"), description = "Kudu master server", required = true)
    var master: String = _

    @Parameter(names = Array("--boss-threads"), description = "Kudu client boss threads")
    var bosses: Integer = _

    @Parameter(names = Array("--worker-threads"), description = "Kudu client worker threads")
    var workers: Integer = _

    @Parameter(names = Array("--disable-statistics"), description = "Disable Kudu client statistics", arity = 0)
    var statistics: java.lang.Boolean = _
  }
}
