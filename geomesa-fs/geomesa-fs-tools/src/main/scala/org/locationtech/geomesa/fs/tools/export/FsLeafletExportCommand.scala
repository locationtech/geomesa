/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.tools.export.FsLeafletExportCommand.FsExportParams
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams, OptionalQueryThreads}
import org.locationtech.geomesa.fs.{FileSystemDataStore, FileSystemDataStoreParams}
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.export.{LeafletExportCommand, LeafletExportParams}

class FsLeafletExportCommand extends LeafletExportCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params = new FsExportParams

  override def connection: Map[String, String] = {
    super.connection + (FileSystemDataStoreParams.ReadThreadsParam.getName -> params.threads.toString)
  }
}

object FsLeafletExportCommand {

  @Parameters(commandDescription = "Export features from a GeoMesa data store and render them in Leaflet")
  class FsExportParams extends LeafletExportParams with FsParams with RequiredTypeNameParam with OptionalQueryThreads
}
