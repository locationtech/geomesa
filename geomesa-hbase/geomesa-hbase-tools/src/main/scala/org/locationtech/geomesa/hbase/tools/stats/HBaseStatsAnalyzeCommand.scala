/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.tools.stats.{StatsAnalyzeCommand, StatsAnalyzeParams}
import org.locationtech.geomesa.tools.{CatalogParam, RequiredTypeNameParam}

class HBaseStatsAnalyzeCommand extends StatsAnalyzeCommand[HBaseDataStore] with HBaseDataStoreCommand{
  override val params = new HBaseStatsAnalyzeParams
}

@Parameters(commandDescription = "Analyze statistics on a GeoMesa feature type")
class HBaseStatsAnalyzeParams extends StatsAnalyzeParams with CatalogParam with RequiredTypeNameParam
