/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.export.AccumuloPlaybackCommand.AccumuloPlaybackParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.export.PlaybackCommand
import org.locationtech.geomesa.tools.export.PlaybackCommand.PlaybackParams

class AccumuloPlaybackCommand extends PlaybackCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloPlaybackParams
}

object AccumuloPlaybackCommand {
  @Parameters(commandDescription = "Playback features from a GeoMesa data store, based on the feature date")
  class AccumuloPlaybackParams extends PlaybackParams with AccumuloDataStoreParams
}
