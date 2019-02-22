/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.redis.data.{RedisDataStore, RedisDataStoreParams}
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand}

/**
  * Abstract class for commands that require a RedisDataStore
  */
trait RedisDataStoreCommand extends DataStoreCommand[RedisDataStore] {

  override def params: RedisDataStoreParams

  override def connection: Map[String, String] =
    Map(
      RedisDataStoreParams.RedisUrlParam.key     -> params.url,
      RedisDataStoreParams.RedisCatalogParam.key -> params.catalog
    )
}

object RedisDataStoreCommand {
  trait RedisDataStoreParams extends CatalogParam {
    @Parameter(names = Array("--url", "-u"), description = "Redis connection URL", required = true)
    var url: String = _
  }
}
