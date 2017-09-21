/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.cassandra.{RowRange, RowValue}
import org.locationtech.geomesa.index.index.AttributeIndex

case object CassandraAttributeIndex
    extends AttributeIndex[CassandraDataStore, CassandraFeature, Seq[RowValue], Seq[RowRange]]
    with CassandraAttributeLayout with CassandraFeatureIndex with CassandraIndexAdapter[Unit]  {
  override val version: Int = 2
}
