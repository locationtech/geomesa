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
import org.locationtech.geomesa.tools.stats.{StatsHistogramCommand, StatsHistogramParams}

class AccumuloStatsHistogramCommand extends StatsHistogramCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloStatsHistogramParams
}

@Parameters(commandDescription = "View or calculate counts of attribute in a GeoMesa feature type, grouped by sorted values")
class AccumuloStatsHistogramParams extends StatsHistogramParams with AccumuloDataStoreParams
