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
import org.locationtech.geomesa.accumulo.tools.ingest.AccumuloDeleteFeaturesCommand.AccumuloDeleteFeaturesParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.data.DeleteFeaturesCommand
import org.locationtech.geomesa.tools.data.DeleteFeaturesCommand.DeleteFeaturesParams

class AccumuloDeleteFeaturesCommand extends DeleteFeaturesCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloDeleteFeaturesParams
}

object AccumuloDeleteFeaturesCommand {
  @Parameters(commandDescription = "Delete features from a GeoMesa schema")
  class AccumuloDeleteFeaturesParams extends DeleteFeaturesParams with AccumuloDataStoreParams
}
