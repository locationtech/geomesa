/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.hbase.tools.data.HBaseUpdateSchemaCommand.HBaseUpdateSchemaParams
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand.UpdateSchemaParams

class HBaseUpdateSchemaCommand extends UpdateSchemaCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseUpdateSchemaParams()
}

object HBaseUpdateSchemaCommand {
  @Parameters(commandDescription = "Update a GeoMesa feature type")
  class HBaseUpdateSchemaParams extends UpdateSchemaParams with HBaseParams with RemoteFilterNotUsedParam
}
