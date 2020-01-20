/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.ingest

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.kudu.tools.ingest.KuduDeleteFeaturesCommand.KuduDeleteFeaturesParams
import org.locationtech.geomesa.tools.data.DeleteFeaturesCommand
import org.locationtech.geomesa.tools.data.DeleteFeaturesCommand.DeleteFeaturesParams

class KuduDeleteFeaturesCommand extends DeleteFeaturesCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduDeleteFeaturesParams
}

object KuduDeleteFeaturesCommand {
  @Parameters(commandDescription = "Delete features from a GeoMesa schema")
  class KuduDeleteFeaturesParams extends DeleteFeaturesParams with KuduParams
}
