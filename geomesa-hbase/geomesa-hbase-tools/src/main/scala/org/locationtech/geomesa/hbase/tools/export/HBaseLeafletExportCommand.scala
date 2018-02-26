/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.hbase.tools.export.HBaseLeafletExportCommand.HBaseLeafletExportParams
import org.locationtech.geomesa.tools.export.{LeafletExportCommand, LeafletExportParams}
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}

class HBaseLeafletExportCommand extends LeafletExportCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseLeafletExportParams
}

object HBaseLeafletExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store and render them in Leaflet")
  class HBaseLeafletExportParams extends LeafletExportParams with HBaseParams with RequiredTypeNameParam
    with OptionalIndexParam with ToggleRemoteFilterParam
}
