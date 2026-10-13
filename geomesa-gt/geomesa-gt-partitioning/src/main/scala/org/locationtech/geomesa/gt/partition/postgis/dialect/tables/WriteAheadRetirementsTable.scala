/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

/**
 * Numeric write-ahead tables whose rows have been copied but whose removal may still be blocked by readers
 */
object WriteAheadRetirementsTable extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(
    s"""CREATE TABLE IF NOT EXISTS ${info.tables.writeAheadRetirements.name.qualified} (
       |  source_partition text PRIMARY KEY,
       |  partition_oid oid NOT NULL UNIQUE,
       |  enqueued timestamp without time zone NOT NULL
       |);""".stripMargin
  )

  override protected def dropStatements(info: TypeInfo): Seq[String] = Seq(
    s"DROP TABLE IF EXISTS ${info.tables.writeAheadRetirements.name.qualified};"
  )
}
