/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.hbase.tools.status.HBaseGetSftConfigCommand.HBaseGetSftConfigParameters
import org.locationtech.geomesa.tools.status.{GetSftConfigCommand, GetSftConfigParams}

class HBaseGetSftConfigCommand extends GetSftConfigCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseGetSftConfigParameters
}

object HBaseGetSftConfigCommand {
  @Parameters(commandDescription = "Get the SimpleFeatureType definition of a schema")
  class HBaseGetSftConfigParameters extends GetSftConfigParams with HBaseParams with ToggleRemoteFilterParam
}
