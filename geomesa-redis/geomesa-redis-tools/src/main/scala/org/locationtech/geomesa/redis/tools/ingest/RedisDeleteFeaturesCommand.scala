/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.ingest

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.redis.tools.ingest.RedisDeleteFeaturesCommand.RedisDeleteFeaturesParams
import org.locationtech.geomesa.tools.data.{DeleteFeaturesCommand, DeleteFeaturesParams}

class RedisDeleteFeaturesCommand extends DeleteFeaturesCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisDeleteFeaturesParams
}

object RedisDeleteFeaturesCommand {
  @Parameters(commandDescription = "Delete features from a GeoMesa schema")
  class RedisDeleteFeaturesParams extends DeleteFeaturesParams with RedisDataStoreParams
}
