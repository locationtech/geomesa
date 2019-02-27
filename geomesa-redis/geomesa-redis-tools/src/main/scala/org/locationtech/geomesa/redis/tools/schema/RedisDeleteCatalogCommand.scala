/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.schema

import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.redis.tools.schema.RedisDeleteCatalogCommand.RedisDeleteCatalogParams
import org.locationtech.geomesa.tools.data.{DeleteCatalogCommand, DeleteCatalogParams}

class RedisDeleteCatalogCommand extends DeleteCatalogCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisDeleteCatalogParams
}

object RedisDeleteCatalogCommand {
  class RedisDeleteCatalogParams extends DeleteCatalogParams with RedisDataStoreParams
}
