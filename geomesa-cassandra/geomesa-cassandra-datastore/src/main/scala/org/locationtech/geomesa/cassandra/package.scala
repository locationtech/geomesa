/***********************************************************************
 * Copyright (c) 2017-2026 IBM
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object cassandra {

  object CassandraSystemProperties {
    // schema changes have to be agreed on by the cluster, so they can take longer than the driver's default request timeout
    val SchemaStatementTimeout: SystemProperty = SystemProperty("geomesa.cassandra.schema.timeout", "30 seconds")
  }

  /**
   * Creates a statement for a schema operation (create, drop or truncate table), which uses
   * `geomesa.cassandra.schema.timeout` instead of the driver's default request timeout
   *
   * @param cql schema statement
   * @return
   */
  def schemaStatement(cql: String): SimpleStatement = {
    val timeout = CassandraSystemProperties.SchemaStatementTimeout.toDuration.get
    SimpleStatement.newInstance(cql).setTimeout(java.time.Duration.ofMillis(timeout.toMillis))
  }

  case class NamedColumn(name: String, i: Int, cType: String, jType: Class[_], partition: Boolean = false)

  case class ColumnSelect(column: NamedColumn, start: Any, end: Any, startInclusive: Boolean, endInclusive: Boolean)
  case class RowSelect(clauses: Seq[ColumnSelect])
}
