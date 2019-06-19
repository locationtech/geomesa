/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreCommand.AccumuloDistributedCommand
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreParams
import org.locationtech.geomesa.accumulo.tools.ingest.AccumuloIngestCommand.AccumuloIngestParams
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams

class AccumuloIngestCommand extends IngestCommand[AccumuloDataStore] with AccumuloDistributedCommand {
  override val params = new AccumuloIngestParams()
}

object AccumuloIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class AccumuloIngestParams extends IngestParams with AccumuloDataStoreParams
}
