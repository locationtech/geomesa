/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.redis.tools.stats.RedisStatsTopKCommand.RedisStatsTopKParams
import org.locationtech.geomesa.tools.stats.{StatsTopKCommand, StatsTopKParams}

class RedisStatsTopKCommand extends StatsTopKCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisStatsTopKParams
}

object RedisStatsTopKCommand {
  @Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
  class RedisStatsTopKParams extends StatsTopKParams with RedisDataStoreParams
}
