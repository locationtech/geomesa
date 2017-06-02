/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.tools.{CatalogParam, KerberosParams, RequiredCredentialsParams}

/**
  * Shared Accumulo-specific command line parameters
  */

trait AccumuloDataStoreParams extends AccumuloConnectionParams with CatalogParam

trait InstanceNameParams extends OptionalZookeepersParam {
  @Parameter(names = Array("-i", "--instance"), description = "Accumulo instance name")
  var instance: String = null

  @Parameter(names = Array("--mock"), description = "Run everything with a mock accumulo instance instead of a real one")
  var mock: Boolean = false
}

trait AccumuloConnectionParams extends InstanceNameParams with RequiredCredentialsParams with KerberosParams {
  @Parameter(names = Array("--auths"), description = "Accumulo authorizations")
  var auths: String = null

  @Parameter(names = Array("--visibilities"), description = "Default feature visibilities")
  var visibilities: String = null
}

trait ZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var zookeepers: String = null
}

trait OptionalZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = null
}

trait AccumuloRasterTableParam {
  @Parameter(names = Array("-t", "--raster-table"), description = "Accumulo table for storing raster data", required = true)
  var table: String = null
}
