/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.status.FsGetSftConfigCommand.FsGetSftConfigParameters
import org.locationtech.geomesa.tools.status.{GetSftConfigCommand, GetSftConfigParams}

class FsGetSftConfigCommand extends GetSftConfigCommand[FileSystemDataStore] with FsDataStoreCommand {
  override val params = new FsGetSftConfigParameters
}

object FsGetSftConfigCommand {
  @Parameters(commandDescription = "Get the SimpleFeatureType definition of a schema")
  class FsGetSftConfigParameters extends FsParams with GetSftConfigParams
}
