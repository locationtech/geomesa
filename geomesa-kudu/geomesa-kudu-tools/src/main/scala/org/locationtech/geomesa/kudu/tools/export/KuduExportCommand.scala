/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.kudu.tools.export.KuduExportCommand.KuduExportParams
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}

class KuduExportCommand extends ExportCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduExportParams
}

object KuduExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class KuduExportParams extends ExportParams with KuduParams with RequiredTypeNameParam
      with OptionalIndexParam
}
