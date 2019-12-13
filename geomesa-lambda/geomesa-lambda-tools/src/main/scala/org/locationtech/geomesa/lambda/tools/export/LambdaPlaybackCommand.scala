/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.lambda.tools.export.LambdaPlaybackCommand.LambdaPlaybackParams
import org.locationtech.geomesa.lambda.tools.{LambdaDataStoreCommand, LambdaDataStoreParams}
import org.locationtech.geomesa.tools.export.PlaybackCommand
import org.locationtech.geomesa.tools.export.PlaybackCommand.PlaybackParams

class LambdaPlaybackCommand extends PlaybackCommand[LambdaDataStore] with LambdaDataStoreCommand {
  override val params = new LambdaPlaybackParams
}

object LambdaPlaybackCommand {
  @Parameters(commandDescription = "Playback features from a GeoMesa data store, based on the feature date")
  class LambdaPlaybackParams extends PlaybackParams with LambdaDataStoreParams
}
