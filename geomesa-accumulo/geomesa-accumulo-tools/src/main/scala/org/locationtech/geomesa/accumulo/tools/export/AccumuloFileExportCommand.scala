/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.export.{FileExportCommand, FileExportParams}
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}

class AccumuloFileExportCommand extends FileExportCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloFileExportParams
}

@Parameters(commandDescription = "Export features from a GeoMesa data store")
class AccumuloFileExportParams extends FileExportParams with AccumuloDataStoreParams
    with RequiredTypeNameParam with OptionalIndexParam
