/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index.legacy

import org.locationtech.geomesa.cassandra.index.CassandraAttributeIndex
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.legacy.{Z2LegacyIndexKeySpace, Z3LegacyIndexKeySpace}
import org.locationtech.geomesa.index.index.z2.XZ2IndexKeySpace
import org.locationtech.geomesa.index.index.z3.XZ3IndexKeySpace
import org.opengis.feature.simple.SimpleFeatureType

case object CassandraAttributeIndexV1 extends CassandraAttributeIndex {

  override val version: Int = 1

  override protected def tieredKeySpace(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]] =
    Seq(Z3LegacyIndexKeySpace, XZ3IndexKeySpace, Z2LegacyIndexKeySpace, XZ2IndexKeySpace).find(_.supports(sft))
}
