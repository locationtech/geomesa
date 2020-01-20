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
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.{KuduDistributedCommand, KuduParams}
import org.locationtech.geomesa.kudu.tools.ingest.KuduIngestCommand.KuduIngestParams
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams

class KuduIngestCommand extends IngestCommand[KuduDataStore] with KuduDistributedCommand {
  override val params = new KuduIngestParams()
}

object KuduIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class KuduIngestParams extends IngestParams with KuduParams
}
