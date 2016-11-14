/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.tools

import com.beust.jcommander.Parameter


trait CassandraConnectionParams {
  @Parameter(names = Array("--contact-point"), description = "contact-point", required = true)
  var contactPoint: String = null

  @Parameter(names = Array("--key-space"), description = "key-space", required = true)
  var keySpace: String = null

  @Parameter(names = Array("--name-space"), description = "name-space", required = true)
  var nameSpace: String = null
}
