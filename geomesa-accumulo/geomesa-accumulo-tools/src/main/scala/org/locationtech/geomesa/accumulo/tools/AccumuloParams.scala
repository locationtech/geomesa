/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
  var instance: String = _

  @Parameter(names = Array("--mock"), description = "Run everything with a mock accumulo instance instead of a real one")
  var mock: Boolean = false
}

trait AccumuloConnectionParams extends InstanceNameParams with RequiredCredentialsParams with KerberosParams {
  @Parameter(names = Array("--auths"), description = "Accumulo authorizations")
  var auths: String = _

  @Parameter(names = Array("--visibilities"), description = "Default feature visibilities")
  var visibilities: String = _
}

trait ZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var zookeepers: String = _
}

trait OptionalZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = _
}

trait AccumuloRasterTableParam {
  @Parameter(names = Array("-t", "--raster-table"), description = "Accumulo table for storing raster data", required = true)
  var table: String = _
}
