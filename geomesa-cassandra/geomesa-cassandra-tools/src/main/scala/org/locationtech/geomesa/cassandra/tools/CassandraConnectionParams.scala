/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.tools.OptionalCredentialsParams

trait CassandraConnectionParams extends OptionalCredentialsParams {
  @Parameter(names = Array("--contact-point", "-P"), description = "Cassandra contact point (address of a Cassandra node)", required = true)
  var contactPoint: String = null

  @Parameter(names = Array("--key-space", "-k"), description = "Cassandra key space (must already exist)", required = true)
  var keySpace: String = null
}
