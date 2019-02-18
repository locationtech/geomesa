/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.ingest

import java.io.File

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.redis.tools.ingest.RedisIngestCommand.RedisIngestParams
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

class RedisIngestCommand extends IngestCommand[RedisDataStore] with RedisDataStoreCommand {

  override val params = new RedisIngestParams()

  override val libjarsFile: String = "org/locationtech/geomesa/redis/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_REDIS_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[RedisDataStore])
  )
}

object RedisIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class RedisIngestParams extends IngestParams with RedisDataStoreParams
}
