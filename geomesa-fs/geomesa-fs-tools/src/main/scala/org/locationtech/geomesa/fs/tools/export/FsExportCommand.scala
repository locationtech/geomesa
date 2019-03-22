/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.export

import com.beust.jcommander.{Parameter, Parameters}
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.export.FsExportCommand.FsExportParams
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams

class FsExportCommand extends ExportCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params = new FsExportParams

  override def connection: Map[String, String] =
    super.connection + (FileSystemDataStoreParams.ReadThreadsParam.getName -> params.threads.toString)
}

object FsExportCommand {

  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class FsExportParams extends ExportParams with FsParams with RequiredTypeNameParam with OptionalQueryThreads

  trait OptionalQueryThreads {
    @Parameter(names = Array("--query-threads"), description = "threads (start with 1)", required = false)
    var threads: java.lang.Integer = 1
  }
}
