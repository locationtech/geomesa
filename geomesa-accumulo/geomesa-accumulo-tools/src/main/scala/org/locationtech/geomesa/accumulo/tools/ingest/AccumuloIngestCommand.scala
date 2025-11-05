/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import com.beust.jcommander.{Parameter, Parameters}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreCommand.AccumuloDistributedCommand
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreParams
import org.locationtech.geomesa.accumulo.tools.ingest.AccumuloIngestCommand.AccumuloIngestParams
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams

class AccumuloIngestCommand extends IngestCommand[AccumuloDataStore] with AccumuloDistributedCommand {

  override val params = new AccumuloIngestParams()

  override def execute(): Unit = {
    super.execute()
    if (params.compact) {
      withDataStore { ds =>
        if (ds.config.generateStats) {
          Command.user.info("Triggering stat table compaction")
          ds.stats.compact(wait = false)
        }
      }
    }
  }
}

object AccumuloIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class AccumuloIngestParams extends IngestParams with AccumuloDataStoreParams {
    @Parameter(
      names = Array("--compact-stats"),
      description = "Compact stats table after ingest, for improved performance",
      arity = 1)
    var compact: Boolean = true
  }
}
