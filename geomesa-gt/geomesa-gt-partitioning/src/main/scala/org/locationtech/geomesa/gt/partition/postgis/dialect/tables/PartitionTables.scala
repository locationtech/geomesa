/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

/**
 * The main and write ahead partitioned tables
 */
object PartitionTables extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] =
    statements(info, info.tables.writeAheadPartitions, "gist") ++
        statements(info, info.tables.mainPartitions, "brin")

  private def statements(info: TypeInfo, table: TableConfig, indexType: String): Seq[String] = {
    // note: don't include storage opts since these are parent partition tables
    val create =
      s"""CREATE TABLE IF NOT EXISTS ${table.name.full} (
         |  LIKE ${info.tables.writeAhead.name.full} INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
         |  CONSTRAINT "${s"${table.name.raw}_pkey"}" PRIMARY KEY (fid, ${info.cols.dtg.name})${table.tablespace.index}
         |) PARTITION BY RANGE(${info.cols.dtg.name})${table.tablespace.table};""".stripMargin
    // note: brin doesn't support 'include' cols
    val geomIndex =
      s"""CREATE INDEX IF NOT EXISTS "${table.name.raw}_${info.cols.geom.raw}"
         |  ON ${table.name.full}
         |  USING $indexType(${info.cols.geom.name})${table.tablespace.table};""".stripMargin
    val dtgIndex =
      s"""CREATE INDEX IF NOT EXISTS "${table.name.raw}_${info.cols.dtg.raw}"
         |  ON ${table.name.full} (${info.cols.dtg.name})${table.tablespace.table};""".stripMargin
    val indices = info.cols.indexed.map { col =>
      s"""CREATE INDEX IF NOT EXISTS s"${table.name.raw}_${col.raw}"
         |  ON ${table.name.full} (${col.name})${table.tablespace.table};""".stripMargin
    }
    Seq(create, geomIndex, dtgIndex) ++ indices
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(info.tables.writeAheadPartitions, info.tables.mainPartitions)
        .map(table => s"DROP TABLE IF EXISTS ${table.name.full};")
}
