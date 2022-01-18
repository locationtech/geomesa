/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.stats.AccumuloStatsCountCommand.AccumuloStatsCountParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.StatsCountCommand
import org.locationtech.geomesa.tools.stats.StatsCountCommand.StatsCountParams

class AccumuloStatsCountCommand extends StatsCountCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloStatsCountParams
}

object AccumuloStatsCountCommand {
  @Parameters(commandDescription = "Estimate or calculate feature counts in a GeoMesa feature type")
  class AccumuloStatsCountParams extends StatsCountParams with AccumuloDataStoreParams with RequiredTypeNameParam
}
