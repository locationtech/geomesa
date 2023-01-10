/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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

  private def tablesAndTypes(info: TypeInfo): Seq[(TableConfig, String)] = {
    Seq(
      info.tables.writeAheadPartitions -> "gist",
      info.tables.mainPartitions       -> "brin",
      info.tables.spillPartitions      -> "gist"
    )
  }

  override protected def createStatements(info: TypeInfo): Seq[String] =
<<<<<<< HEAD
    tablesAndTypes(info).flatMap { case (table, indexType) => statements(info, table, indexType) }
=======
    statements(info, info.tables.writeAheadPartitions, "gist") ++
        statements(info, info.tables.mainPartitions, "brin") ++
        statements(info, info.tables.spillPartitions, "gist")
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

  private def statements(info: TypeInfo, table: TableConfig, indexType: String): Seq[String] = {
    // note: don't include storage opts since these are parent partition tables
    val (tableTs, indexTs) = table.tablespace match {
      case None => ("", "")
      case Some(ts) => (s" TABLESPACE ${ts.quoted}", s" USING INDEX TABLESPACE ${ts.quoted}")
    }
    val create =
      s"""CREATE TABLE IF NOT EXISTS ${table.name.qualified} (
         |  LIKE ${info.tables.writeAhead.name.qualified} INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
         |  CONSTRAINT ${escape(table.name.raw, "pkey")} PRIMARY KEY (fid, ${info.cols.dtg.quoted})$indexTs
         |) PARTITION BY RANGE(${info.cols.dtg.quoted})$tableTs;""".stripMargin
    val pagesPerRange =
      if (indexType == "brin") { s" with (pages_per_range = ${info.partitions.pagesPerRange})" } else { "" }
    // note: brin doesn't support 'include' cols
    val geomIndex =
      s"""CREATE INDEX IF NOT EXISTS ${escape(table.name.raw, info.cols.geom.raw)}
         |  ON ${table.name.qualified}
         |  USING $indexType(${info.cols.geom.quoted})$pagesPerRange$tableTs;""".stripMargin
    val dtgIndex =
      s"""CREATE INDEX IF NOT EXISTS ${escape(table.name.raw, info.cols.dtg.raw)}
         |  ON ${table.name.qualified} (${info.cols.dtg.quoted})$tableTs;""".stripMargin
    val indices = info.cols.indexed.map { col =>
      s"""CREATE INDEX IF NOT EXISTS ${escape(table.name.raw, col.raw)}
         |  ON ${table.name.qualified} (${col.quoted})$tableTs;""".stripMargin
    }
    Seq(create, geomIndex, dtgIndex) ++ indices
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    tablesAndTypes(info).map { case (table, _) => s"DROP TABLE IF EXISTS ${table.name.qualified};" }
}
