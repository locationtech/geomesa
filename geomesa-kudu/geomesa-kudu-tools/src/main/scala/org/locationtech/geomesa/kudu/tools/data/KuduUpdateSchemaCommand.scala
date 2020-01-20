/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.kudu.tools.data.KuduUpdateSchemaCommand.KuduUpdateSchemaParams
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand.UpdateSchemaParams

class KuduUpdateSchemaCommand extends UpdateSchemaCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduUpdateSchemaParams()
}

object KuduUpdateSchemaCommand {
  @Parameters(commandDescription = "Update a GeoMesa feature type")
  class KuduUpdateSchemaParams extends UpdateSchemaParams with KuduParams
}
