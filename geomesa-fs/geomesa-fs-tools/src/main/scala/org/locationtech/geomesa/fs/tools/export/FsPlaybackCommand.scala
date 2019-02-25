/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.export.FsExportCommand.OptionalQueryThreads
import org.locationtech.geomesa.fs.tools.export.FsPlaybackCommand.FsPlaybackParams
import org.locationtech.geomesa.tools.export.PlaybackCommand
import org.locationtech.geomesa.tools.export.PlaybackCommand.PlaybackParams

class FsPlaybackCommand extends PlaybackCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params = new FsPlaybackParams

  override def connection: Map[String, String] =
    super.connection + (FileSystemDataStoreParams.ReadThreadsParam.getName -> params.threads.toString)
}

object FsPlaybackCommand {
  @Parameters(commandDescription = "Playback features from a GeoMesa data store, based on the feature date")
  class FsPlaybackParams extends PlaybackParams with FsParams with OptionalQueryThreads
}
