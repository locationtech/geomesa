/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.kudu.tools.data.KuduRemoveSchemaCommand.KuduRemoveSchemaParams
import org.locationtech.geomesa.tools.data.{RemoveSchemaCommand, RemoveSchemaParams}

class KuduRemoveSchemaCommand extends RemoveSchemaCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduRemoveSchemaParams
}

object KuduRemoveSchemaCommand {
  @Parameters(commandDescription = "Remove a schema and all associated features")
  class KuduRemoveSchemaParams extends RemoveSchemaParams with KuduParams
}
