/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, RemoteFilterNotUsedParam}
import org.locationtech.geomesa.hbase.tools.data.HBaseDeleteCatalogCommand.HBaseDeleteCatalogParams
import org.locationtech.geomesa.tools.data.{DeleteCatalogCommand, DeleteCatalogParams}

class HBaseDeleteCatalogCommand extends DeleteCatalogCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseDeleteCatalogParams
}

object HBaseDeleteCatalogCommand {
  @Parameters(commandDescription = "Delete a GeoMesa catalog completely (and all features in it)")
  class HBaseDeleteCatalogParams extends DeleteCatalogParams with HBaseParams with RemoteFilterNotUsedParam
}
