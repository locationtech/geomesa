/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.export.AccumuloExportCommand.AccumuloExportParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}

class AccumuloExportCommand extends ExportCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloExportParams
}

object AccumuloExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class AccumuloExportParams extends ExportParams with AccumuloDataStoreParams
      with RequiredTypeNameParam with OptionalIndexParam
}
