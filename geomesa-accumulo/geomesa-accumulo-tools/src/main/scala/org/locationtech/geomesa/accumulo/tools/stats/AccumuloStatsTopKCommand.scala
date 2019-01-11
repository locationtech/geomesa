/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.stats.{StatsTopKCommand, StatsTopKParams}

class AccumuloStatsTopKCommand extends StatsTopKCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloStatsTopKParams
}

@Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
class AccumuloStatsTopKParams extends StatsTopKParams with AccumuloDataStoreParams
