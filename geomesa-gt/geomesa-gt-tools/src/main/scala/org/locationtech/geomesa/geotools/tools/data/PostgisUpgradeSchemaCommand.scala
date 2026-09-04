/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.GeoToolsDataStoreParams
import org.locationtech.geomesa.geotools.tools.data.PostgisUpgradeSchemaCommand.PostgisUpgradeSchemaParams
import org.locationtech.geomesa.gt.partition.postgis.PartitionedPostgisDataStore
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

class PostgisUpgradeSchemaCommand extends GeoToolsDataStoreCommand {

  override val params = new PostgisUpgradeSchemaParams()

  override val name: String = "partition-upgrade"

  override def execute(): Unit = withDataStore { case ds: PartitionedPostgisDataStore =>
    Command.user.info(s"Running upgrade on schema: ${params.featureName}")
    ds.upgrade(ds.getSchema(params.featureName))
    Command.user.info("Upgrade complete")
  }
}

object PostgisUpgradeSchemaCommand {
  @Parameters(commandDescription = "Update the GeoMesa partitioning functions to the latest version")
  class PostgisUpgradeSchemaParams extends GeoToolsDataStoreParams with RequiredTypeNameParam
}

