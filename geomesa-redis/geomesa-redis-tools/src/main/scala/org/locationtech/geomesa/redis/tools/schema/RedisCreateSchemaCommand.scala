/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.schema

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.redis.tools.schema.RedisCreateSchemaCommand.RedisCreateSchemaParams
import org.locationtech.geomesa.tools.data.CreateSchemaCommand
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.CreateSchemaParams

class RedisCreateSchemaCommand extends CreateSchemaCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisCreateSchemaParams()
}

object RedisCreateSchemaCommand {
  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class RedisCreateSchemaParams extends CreateSchemaParams with RedisDataStoreParams
}
