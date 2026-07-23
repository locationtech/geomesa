/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.tools.status

import com.beust.jcommander.Parameters
import com.typesafe.scalalogging.Logger
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.status.DescribeSchemaCommand
import org.locationtech.geomesa.trino.datastore.TrinoDataStore
import org.locationtech.geomesa.trino.tools.TrinoDataStoreCommand
import org.locationtech.geomesa.trino.tools.TrinoDataStoreCommand.TrinoParams
import org.locationtech.geomesa.trino.tools.status.TrinoDescribeSchemaCommand.TrinoDescribeSchemaParams

class TrinoDescribeSchemaCommand extends DescribeSchemaCommand[TrinoDataStore] with TrinoDataStoreCommand {
  override val params = new TrinoDescribeSchemaParams

  override protected def describe(ds: TrinoDataStore, sft: SimpleFeatureType, logger: Logger): Unit = {
    super.describe(ds, sft, logger)
    // TODO describe partition scheme, other things?
  }
}

object TrinoDescribeSchemaCommand {
  @Parameters(commandDescription = "Describe the attributes of a given GeoMesa feature type")
  class TrinoDescribeSchemaParams extends TrinoParams with RequiredTypeNameParam
}
