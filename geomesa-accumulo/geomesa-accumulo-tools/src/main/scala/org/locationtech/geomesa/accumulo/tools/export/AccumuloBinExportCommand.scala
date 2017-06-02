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
import org.locationtech.geomesa.tools.export.BinExportCommand.BinExportParams
import org.locationtech.geomesa.tools.export.{BinExportCommand, ExportCommand}

@deprecated("AccumuloExportCommand")
class AccumuloBinExportCommand extends BinExportCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloBinExportParams
  override val delegate: ExportCommand[AccumuloDataStore] = new AccumuloExportCommand
}

@Parameters(commandDescription = "Export features from a GeoMesa data store in a binary format")
class AccumuloBinExportParams extends BinExportParams with AccumuloDataStoreParams
