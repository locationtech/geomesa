/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.ingest.AccumuloUpdateFeaturesCommand.AccumuloUpdateFeaturesParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.ingest.UpdateFeaturesCommand
import org.locationtech.geomesa.tools.ingest.UpdateFeaturesCommand.UpdateFeaturesParams

class AccumuloUpdateFeaturesCommand extends UpdateFeaturesCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params: AccumuloUpdateFeaturesParams = new AccumuloUpdateFeaturesParams()
}

object AccumuloUpdateFeaturesCommand {
  @Parameters(commandDescription = "Update attributes or visibilities of features in GeoMesa.")
  class AccumuloUpdateFeaturesParams extends UpdateFeaturesParams with AccumuloDataStoreParams
}
