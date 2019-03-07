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
import org.locationtech.geomesa.redis.tools.export.RedisExportCommand.RedisExportParams
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}

class RedisExportCommand extends ExportCommand[RedisDataStore] with RedisDataStoreCommand {
  override val params = new RedisExportParams
}

object RedisExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class RedisExportParams extends ExportParams with RedisDataStoreParams
      with RequiredTypeNameParam with OptionalIndexParam
}
