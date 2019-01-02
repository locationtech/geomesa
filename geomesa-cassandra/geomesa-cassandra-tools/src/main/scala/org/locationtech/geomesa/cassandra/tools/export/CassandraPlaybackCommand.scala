/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.tools.export.CassandraPlaybackCommand.CassandraPlaybackParams
import org.locationtech.geomesa.cassandra.tools.{CassandraConnectionParams, CassandraDataStoreCommand}
import org.locationtech.geomesa.tools.CatalogParam
import org.locationtech.geomesa.tools.export.PlaybackCommand
import org.locationtech.geomesa.tools.export.PlaybackCommand.PlaybackParams

class CassandraPlaybackCommand extends PlaybackCommand[CassandraDataStore] with CassandraDataStoreCommand {
  override val params = new CassandraPlaybackParams
}

object CassandraPlaybackCommand {
  @Parameters(commandDescription = "Playback features from a GeoMesa data store, based on the feature date")
  class CassandraPlaybackParams extends PlaybackParams with CassandraConnectionParams with CatalogParam
}
