/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools.`export`

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.arrow.data.ArrowDataStore
import org.locationtech.geomesa.arrow.tools.ArrowDataStoreCommand
import org.locationtech.geomesa.arrow.tools.ArrowDataStoreCommand.UrlParam
import org.locationtech.geomesa.arrow.tools.export.ArrowExportCommand.ArrowExportParams
import org.locationtech.geomesa.tools.ProvidedTypeNameParam
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams

class ArrowExportCommand extends ExportCommand[ArrowDataStore] with ArrowDataStoreCommand {
  override val params = new ArrowExportParams
}

object ArrowExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class ArrowExportParams extends ExportParams with UrlParam with ProvidedTypeNameParam
}
