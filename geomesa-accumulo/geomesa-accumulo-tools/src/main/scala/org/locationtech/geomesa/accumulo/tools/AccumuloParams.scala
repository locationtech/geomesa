/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.tools.{CatalogParam, CredentialsParams, KerberosParams}

/**
  * Shared Accumulo-specific command line parameters
  */

trait AccumuloDataStoreParams extends AccumuloConnectionParams with CatalogParam

trait InstanceNameParams extends ZookeepersParam {
  @Parameter(names = Array("-i", "--instance"), description = "Accumulo instance name")
  var instance: String = _
}

trait AccumuloConnectionParams extends InstanceNameParams with CredentialsParams with KerberosParams {
  @Parameter(names = Array("--auths"), description = "Accumulo authorizations")
  var auths: String = _
}

trait ZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = _

  @Parameter(names = Array("--zookeepers-timeout"), description = "Zookeepers timeout")
  var zkTimeout: String = _
}
