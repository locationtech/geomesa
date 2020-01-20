/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools

import java.io.File

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.redis.data.{RedisDataStore, RedisDataStoreParams}
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

/**
  * Abstract class for commands that require a RedisDataStore
  */
trait RedisDataStoreCommand extends DataStoreCommand[RedisDataStore] {

  override def params: RedisDataStoreParams

  override def connection: Map[String, String] = {
    Map(
      RedisDataStoreParams.RedisUrlParam.key     -> params.url,
      RedisDataStoreParams.RedisCatalogParam.key -> params.catalog
    )
  }
}

object RedisDataStoreCommand {

  trait RedisDistributedCommand extends RedisDataStoreCommand with DistributedCommand  {

    abstract override def libjarsFiles: Seq[String] =
      Seq("org/locationtech/geomesa/redis/tools/redis-libjars.list") ++ super.libjarsFiles

    abstract override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_REDIS_HOME", "lib"),
      () => ClassPathUtils.getJarsFromClasspath(classOf[RedisDataStore])
    ) ++ super.libjarsPaths
  }

  trait RedisDataStoreParams extends CatalogParam {
    @Parameter(names = Array("--url", "-u"), description = "Redis connection URL", required = true)
    var url: String = _
  }
}
