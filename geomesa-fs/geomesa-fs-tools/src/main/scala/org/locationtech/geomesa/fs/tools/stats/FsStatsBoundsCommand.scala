/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.stats.FsStatsBoundsCommand.FsStatsBoundsParams
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.{StatsBoundsCommand, StatsBoundsParams}

class FsStatsBoundsCommand extends StatsBoundsCommand[FileSystemDataStore] with FsDataStoreCommand {
  override val params = new FsStatsBoundsParams
}

object FsStatsBoundsCommand {
  @Parameters(commandDescription = "View or calculate bounds on attributes in a GeoMesa feature type")
  class FsStatsBoundsParams extends StatsBoundsParams with FsParams with RequiredTypeNameParam
}
