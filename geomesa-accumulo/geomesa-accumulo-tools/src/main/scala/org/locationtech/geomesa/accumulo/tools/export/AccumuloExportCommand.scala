/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.export.{ExportCommand, ExportParams}

class AccumuloExportCommand extends ExportCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloExportParams
}

@Parameters(commandDescription = "Export features from a GeoMesa data store")
class AccumuloExportParams extends ExportParams
  with AccumuloDataStoreParams
  with RequiredTypeNameParam
