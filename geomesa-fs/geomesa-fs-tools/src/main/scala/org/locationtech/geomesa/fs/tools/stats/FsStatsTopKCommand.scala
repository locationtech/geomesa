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
import org.locationtech.geomesa.fs.tools.stats.FsStatsTopKCommand.FsStatsTopKParams
import org.locationtech.geomesa.tools.stats.{StatsTopKCommand, StatsTopKParams}

class FsStatsTopKCommand extends StatsTopKCommand[FileSystemDataStore] with FsDataStoreCommand {
  override val params = new FsStatsTopKParams
}

object FsStatsTopKCommand {
  @Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
  class FsStatsTopKParams extends StatsTopKParams with FsParams
}
