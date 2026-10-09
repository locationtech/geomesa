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
 * Stores write-ahead partition ranges that have been copied into a main partition but have not yet been detached
 */
object WriteAheadMigrationsTable extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(
    s"""CREATE TABLE IF NOT EXISTS ${info.tables.writeAheadMigrations.name.qualified} (
       |  source_partition text PRIMARY KEY,
       |  partition_start timestamp without time zone NOT NULL,
       |  partition_end timestamp without time zone NOT NULL,
       |  enqueued timestamp without time zone NOT NULL
       |);""".stripMargin,
    s"""CREATE INDEX IF NOT EXISTS ${escape(info.tables.writeAheadMigrations.name.raw, "range")}
       |  ON ${info.tables.writeAheadMigrations.name.qualified} (partition_start, partition_end);""".stripMargin
  )

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP TABLE IF EXISTS ${info.tables.writeAheadMigrations.name.qualified};")
}
