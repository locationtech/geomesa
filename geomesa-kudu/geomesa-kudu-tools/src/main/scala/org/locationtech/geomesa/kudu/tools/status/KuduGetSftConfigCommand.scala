/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.kudu.tools.status.KuduGetSftConfigCommand.KuduGetSftConfigParameters
import org.locationtech.geomesa.tools.status.{GetSftConfigCommand, GetSftConfigParams}

class KuduGetSftConfigCommand extends GetSftConfigCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduGetSftConfigParameters
}

object KuduGetSftConfigCommand {
  @Parameters(commandDescription = "Get the SimpleFeatureType definition of a schema")
  class KuduGetSftConfigParameters extends GetSftConfigParams with KuduParams
}
