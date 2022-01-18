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
import org.locationtech.geomesa.redis.tools.stats.RedisStatsCountCommand.RedisStatsCountParams
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.StatsCountCommand
import org.locationtech.geomesa.tools.stats.StatsCountCommand.StatsCountParams

class RedisStatsCountCommand extends StatsCountCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisStatsCountParams
}

object RedisStatsCountCommand {
  @Parameters(commandDescription = "Estimate or calculate feature counts in a GeoMesa feature type")
  class RedisStatsCountParams extends StatsCountParams with RedisDataStoreParams with RequiredTypeNameParam
}
