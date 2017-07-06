/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.tools.status.GetTypeNamesCommand

class FsGetTypeNamesCommand extends GetTypeNamesCommand[FileSystemDataStore] with FsDataStoreCommand {
  override val params = new FsGetTypeNamesParams()
}

@Parameters(commandDescription = "List GeoMesa feature type for a given Fs resource")
class FsGetTypeNamesParams extends FsParams
