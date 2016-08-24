/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.tools.common.OptionalZookeepersParam

/**
  * Shared Accumulo-specific command line parameters
  */

trait OptionalAccumuloSharedTablesParam {
  @Parameter(names = Array("--use-shared-tables"), description = "Use shared tables in Accumulo for feature storage (true/false)", arity = 1)
  var useSharedTables: Boolean = true //default to true in line with datastore
}

trait AccumuloRasterTableParam {
  @Parameter(names = Array("-t", "--raster-table"), description = "Accumulo table for storing raster data", required = true)
  var table: String = null
}

trait AccumuloConnectionParams extends OptionalZookeepersParam {
  @Parameter(names = Array("-u", "--user"), description = "Accumulo user name", required = true)
  var user: String = null

  @Parameter(names = Array("-p", "--password"), description = "Accumulo password (will prompt if not supplied)")
  var password: String = null

  @Parameter(names = Array("-i", "--instance"), description = "Accumulo instance name")
  var instance: String = null

  @Parameter(names = Array("--auths"), description = "Accumulo authorizations")
  var auths: String = null

  @Parameter(names = Array("--visibilities"), description = "Default feature visibilities")
  var visibilities: String = null

  @Parameter(names = Array("--mock"), description = "Run everything with a mock accumulo instance instead of a real one")
  var useMock: Boolean = false
}

trait GeoMesaConnectionParams extends AccumuloConnectionParams {
  @Parameter(names = Array("-c", "--catalog"), description = "Catalog table name for GeoMesa", required = true)
  var catalog: String = null
}
