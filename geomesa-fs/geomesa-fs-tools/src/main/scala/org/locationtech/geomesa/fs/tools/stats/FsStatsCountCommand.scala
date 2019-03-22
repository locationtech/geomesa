/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.stats.FsStatsCountCommand.FsStatsCountParams
import org.locationtech.geomesa.tools.stats.{StatsCountCommand, StatsCountParams}

class FsStatsCountCommand extends StatsCountCommand[FileSystemDataStore] with FsDataStoreCommand {
  override val params = new FsStatsCountParams
}

object FsStatsCountCommand {
  @Parameters(commandDescription = "Estimate or calculate feature counts in a GeoMesa feature type")
  class FsStatsCountParams extends StatsCountParams with FsParams
}
