/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.export

import com.beust.jcommander.Parameters
import org.geotools.data.DataStore
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.GeoToolsDataStoreParams
import org.locationtech.geomesa.geotools.tools.export.GeoToolsPlaybackCommand.GeoToolsPlaybackParams
import org.locationtech.geomesa.tools.export.PlaybackCommand
import org.locationtech.geomesa.tools.export.PlaybackCommand.PlaybackParams

class GeoToolsPlaybackCommand extends PlaybackCommand[DataStore] with GeoToolsDataStoreCommand {
  override val params: GeoToolsPlaybackParams = new GeoToolsPlaybackParams
}

object GeoToolsPlaybackCommand {
  @Parameters(commandDescription = "Playback features from a data store, based on the feature date")
  class GeoToolsPlaybackParams extends PlaybackParams with GeoToolsDataStoreParams
}
