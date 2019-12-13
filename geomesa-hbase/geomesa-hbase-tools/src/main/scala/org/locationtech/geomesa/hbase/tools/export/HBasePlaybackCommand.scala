/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.hbase.tools.export.HBasePlaybackCommand.HBasePlaybackParams
import org.locationtech.geomesa.tools.export.PlaybackCommand
import org.locationtech.geomesa.tools.export.PlaybackCommand.PlaybackParams

class HBasePlaybackCommand extends PlaybackCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBasePlaybackParams
}

object HBasePlaybackCommand {
  @Parameters(commandDescription = "Playback features from a GeoMesa data store, based on the feature date")
  class HBasePlaybackParams extends PlaybackParams with HBaseParams with ToggleRemoteFilterParam
}
