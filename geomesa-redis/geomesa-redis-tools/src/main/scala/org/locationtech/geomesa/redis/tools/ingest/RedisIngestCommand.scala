/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.ingest

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.{RedisDataStoreParams, RedisDistributedCommand}
import org.locationtech.geomesa.redis.tools.ingest.RedisIngestCommand.RedisIngestParams
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams

class RedisIngestCommand extends IngestCommand[RedisDataStore] with RedisDistributedCommand {
  override val params = new RedisIngestParams()
}

object RedisIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class RedisIngestParams extends IngestParams with RedisDataStoreParams
}
