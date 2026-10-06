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
 * The main and write ahead partitioned tables
 */
object PartitionTables extends SqlStatements {

  private def tables(info: TypeInfo): Seq[TableConfig] = Seq(
    info.tables.writeAheadPartitions,
    info.tables.mainPartitions,
    info.tables.spillPartitions,
  )

  override protected def createStatements(info: TypeInfo): Seq[String] = tables(info).flatMap(statements(info, _))

  private def statements(info: TypeInfo, table: TableConfig): Seq[String] = {
    // note: don't include storage opts since these are parent partition tables
    val (tableTs, indexTs) = table.tablespace match {
      case None => ("", "")
      case Some(ts) => (s"TABLESPACE ${ts.quoted}", s"USING INDEX TABLESPACE ${ts.quoted}")
    }

    val logging = if (table.logged) { "" } else { "UNLOGGED" }
    val create =
      s"""CREATE $logging TABLE IF NOT EXISTS ${table.name.qualified} (
         |  LIKE ${info.tables.writeAhead.name.qualified} INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
         |  CONSTRAINT ${escape(table.name.raw, "pkey")} PRIMARY KEY (${info.cols.fid.quoted}, ${info.cols.dtg.quoted}) $indexTs
         |) PARTITION BY RANGE(${info.cols.dtg.quoted}) $tableTs;""".stripMargin
    // note: partitions inherit these indices when they're attached, so they must be declared here
    val indices = info.cols.filter(table.name).indices.map { index =>
      s"""CREATE INDEX IF NOT EXISTS ${escape(table.name.raw, index.cols.map(_.raw).mkString("_"))}
         |  ON ${table.name.qualified}
         |  ${index.using} (${index.cols.map(_.quoted).mkString(", ")} ${index.opclass}) ${index.includes} ${index.storageOpts} $tableTs;""".stripMargin
    }
    Seq(create) ++ indices
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    tables(info).map(table => s"DROP TABLE IF EXISTS ${table.name.qualified};")
}
