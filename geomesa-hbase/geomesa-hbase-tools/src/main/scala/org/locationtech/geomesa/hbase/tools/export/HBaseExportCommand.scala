/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.tools.{CatalogParam, RequiredTypeNameParam}
import org.locationtech.geomesa.tools.export.{ExportCommand, ExportParams}

class HBaseExportCommand extends ExportCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseExportParams
}

@Parameters(commandDescription = "Export features from a GeoMesa data store")
class HBaseExportParams extends ExportParams with CatalogParam with RequiredTypeNameParam
