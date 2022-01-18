/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.redis.tools.stats.RedisStatsHistogramCommand.RedisStatsHistogramParams
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.StatsHistogramCommand
import org.locationtech.geomesa.tools.stats.StatsHistogramCommand.StatsHistogramParams

class RedisStatsHistogramCommand extends StatsHistogramCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisStatsHistogramParams
}

object RedisStatsHistogramCommand {
  @Parameters(commandDescription = "View or calculate counts of attribute in a GeoMesa feature type, grouped by sorted values")
  class RedisStatsHistogramParams extends StatsHistogramParams with RedisDataStoreParams with RequiredTypeNameParam
}
