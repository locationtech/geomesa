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
import org.locationtech.geomesa.fs.tools.stats.FsStatsHistogramCommand.FsStatsHistogramParams
import org.locationtech.geomesa.tools.stats.{StatsHistogramCommand, StatsHistogramParams}

class FsStatsHistogramCommand extends StatsHistogramCommand[FileSystemDataStore] with FsDataStoreCommand {
  override val params = new FsStatsHistogramParams
}

object FsStatsHistogramCommand {
  @Parameters(commandDescription = "View or calculate counts of attribute in a GeoMesa feature type, grouped by sorted values")
  class FsStatsHistogramParams extends StatsHistogramParams with FsParams
}
