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
import org.locationtech.geomesa.hbase.tools.data.HBaseDeleteFeaturesCommand.HBaseDeleteFeaturesParams
import org.locationtech.geomesa.tools.data.{DeleteFeaturesCommand, DeleteFeaturesParams}

class HBaseDeleteFeaturesCommand extends DeleteFeaturesCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseDeleteFeaturesParams
}

object HBaseDeleteFeaturesCommand {
  @Parameters(commandDescription = "Delete features from a GeoMesa schema")
  class HBaseDeleteFeaturesParams extends DeleteFeaturesParams with HBaseParams with RemoteFilterNotUsedParam
}
