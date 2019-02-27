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
import org.locationtech.geomesa.redis.tools.stats.RedisStatsBoundsCommand.RedisStatsBoundsParams
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.{StatsBoundsCommand, StatsBoundsParams}

class RedisStatsBoundsCommand extends StatsBoundsCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisStatsBoundsParams
}

object RedisStatsBoundsCommand {
  @Parameters(commandDescription = "View or calculate bounds on attributes in a GeoMesa feature type")
  class RedisStatsBoundsParams extends StatsBoundsParams with RedisDataStoreParams with RequiredTypeNameParam
}
