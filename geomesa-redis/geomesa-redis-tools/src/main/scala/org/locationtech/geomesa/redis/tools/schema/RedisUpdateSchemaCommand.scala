/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.schema

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.redis.tools.schema.RedisUpdateSchemaCommand.RedisUpdateSchemaParams
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand.UpdateSchemaParams

class RedisUpdateSchemaCommand extends UpdateSchemaCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisUpdateSchemaParams()
}

object RedisUpdateSchemaCommand {
  @Parameters(commandDescription = "Update a GeoMesa feature type")
  class RedisUpdateSchemaParams extends UpdateSchemaParams with RedisDataStoreParams
}
