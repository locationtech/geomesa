/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.redis.tools.export.RedisExplainCommand.RedisExplainParams
import org.locationtech.geomesa.tools.status.{ExplainCommand, ExplainParams}

class RedisExplainCommand extends ExplainCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisExplainParams()
}

object RedisExplainCommand {
  @Parameters(commandDescription = "Explain how a GeoMesa query will be executed")
  class RedisExplainParams extends ExplainParams with RedisDataStoreParams
}
