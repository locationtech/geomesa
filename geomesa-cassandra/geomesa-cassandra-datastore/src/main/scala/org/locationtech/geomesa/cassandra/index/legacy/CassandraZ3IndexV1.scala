/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index.legacy

import org.locationtech.geomesa.cassandra.index.CassandraZ3Index
import org.locationtech.geomesa.index.index.legacy.Z3LegacyIndexKeySpace
import org.locationtech.geomesa.index.index.z3.Z3IndexKeySpace

case object CassandraZ3IndexV1 extends CassandraZ3Index {

  override val version: Int = 1

  override protected val keySpace: Z3IndexKeySpace = Z3LegacyIndexKeySpace
}
