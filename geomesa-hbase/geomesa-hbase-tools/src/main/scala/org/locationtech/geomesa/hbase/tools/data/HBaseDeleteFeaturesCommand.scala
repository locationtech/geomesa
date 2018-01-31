/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.RemoteFilterNotUsedParam
import org.locationtech.geomesa.hbase.tools.data.HBaseDeleteFeaturesCommand.HBaseDeleteFeaturesParams
import org.locationtech.geomesa.tools.CatalogParam
import org.locationtech.geomesa.tools.data.{DeleteFeaturesCommand, DeleteFeaturesParams}

class HBaseDeleteFeaturesCommand extends DeleteFeaturesCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseDeleteFeaturesParams
}

object HBaseDeleteFeaturesCommand {
  @Parameters(commandDescription = "Delete features from a table in GeoMesa. Does not delete any tables or schema information.")
  class HBaseDeleteFeaturesParams extends DeleteFeaturesParams with CatalogParam with RemoteFilterNotUsedParam
}
