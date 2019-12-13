/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object cassandra {

  case class NamedColumn(name: String, i: Int, cType: String, jType: Class[_], partition: Boolean = false)

  case class ColumnSelect(column: NamedColumn, start: Any, end: Any, startInclusive: Boolean, endInclusive: Boolean)
  case class RowSelect(clauses: Seq[ColumnSelect])

  object CassandraSystemProperties {
    val ReadTimeoutMillis       = SystemProperty("geomesa.cassandra.read.timeout", "30 seconds")
    val ConnectionTimeoutMillis = SystemProperty("geomesa.cassandra.connection.timeout", "30 seconds")
  }
}
