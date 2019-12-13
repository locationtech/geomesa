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
import org.locationtech.geomesa.hbase.tools.status.HBaseExplainCommand.HBaseExplainParams
import org.locationtech.geomesa.tools.status.{ExplainCommand, ExplainParams}

class HBaseExplainCommand extends ExplainCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseExplainParams()
}

object HBaseExplainCommand {
  @Parameters(commandDescription = "Explain how a GeoMesa query will be executed")
  class HBaseExplainParams extends ExplainParams with HBaseParams with ToggleRemoteFilterParam
}
