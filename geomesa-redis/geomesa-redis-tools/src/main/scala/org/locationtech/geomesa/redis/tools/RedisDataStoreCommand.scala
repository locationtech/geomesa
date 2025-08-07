/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.redis.data.{RedisDataStore, RedisDataStoreParams}
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import java.io.File

/**
  * Abstract class for commands that require a RedisDataStore
  */
trait RedisDataStoreCommand extends DataStoreCommand[RedisDataStore] {

  override def params: RedisDataStoreParams

  override def connection: Map[String, String] = {
    Map(
      RedisDataStoreParams.RedisUrlParam.key     -> params.url,
      RedisDataStoreParams.RedisCatalogParam.key -> params.catalog,
      RedisDataStoreParams.RedisClusterParam.key -> params.cluster.toString,
      RedisDataStoreParams.AuthsParam.key        -> params.auths,
    ).filter(_._2 != null)
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

    @Parameter(names = Array("--cluster", "-l"), description = "Redis cluster enabled?", required = false)
    var cluster: Boolean = false

    @Parameter(names = Array("--auths"), description = "Authorizations used to read data")
    var auths: String = _
  }
}
