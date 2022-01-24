/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

import java.util.Locale

/**
 * Write ahead log, partitioned using inheritance to avoid any restraints on data overlap between partitions.
 * All writes get directed to the main partition, which is identified with the suffix `_writes`. Every 10 minutes,
 * the current writes partition is renamed based on a sequence number, and a new writes partition is created
 */
object WriteAheadTable extends SqlStatements {

  def writesPartition(info: TypeInfo): String =
    TableName(info.schema, info.tables.writeAhead.name.raw + "_writes").full

  def writesPartitionRaw(info: TypeInfo): String =
    TableName(info.schema, info.tables.writeAhead.name.raw + "_writes").raw

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    val table = info.tables.writeAhead
    val partition = writesPartition(info)
    val seq = s"""CREATE SEQUENCE IF NOT EXISTS "${table.name.raw}_seq" AS smallint MINVALUE 1 MAXVALUE 999 CYCLE;"""
    // rename the table created by the JdbcDataStore to be the write ahead table
    val rename = s"ALTER TABLE ${info.tables.view.name.full} RENAME TO ${table.name.unqualified};"
    // drop the index created by the JDBC data store on the parent table, as all the data is in the inherited table
    val dropIndices = info.cols.geoms.map { col =>
      s"""DROP INDEX IF EXISTS "spatial_${info.tables.view.name.raw}_${col.raw.toLowerCase(Locale.US)}";"""
    }
    val move = if (table.tablespace.table.isEmpty) { Seq.empty } else {
      Seq(s"ALTER TABLE ${table.name.full} SET${table.tablespace.table};")
    }
    val child =
      s"""CREATE TABLE IF NOT EXISTS $partition (
         |  CONSTRAINT "${table.name.raw}_000_pkey" PRIMARY KEY (fid, ${info.cols.dtg.name})${table.tablespace.index}
         |) INHERITS (${table.name.full})${table.storage}${table.tablespace.table};""".stripMargin
    val dtgIndex =
      s"""CREATE INDEX IF NOT EXISTS "${table.name.raw}_${info.cols.dtg.raw}_000"
         |  ON $partition (${info.cols.dtg.name})${table.tablespace.table};""".stripMargin
    val geomIndices = info.cols.geoms.map { col =>
      s"""CREATE INDEX IF NOT EXISTS "spatial_${table.name.raw}_${col.raw.toLowerCase(Locale.US)}_000"
         |  ON $partition USING gist(${col.name})${table.tablespace.table};""".stripMargin
    }
    val indices = info.cols.indexed.map { col =>
      s"""CREATE INDEX IF NOT EXISTS "${table.name.raw}_${col.raw}_000"
         |  ON $partition (${col.name})${table.tablespace.table};""".stripMargin
    }
    Seq(seq, rename) ++ dropIndices ++ move ++ Seq(child, dtgIndex) ++ geomIndices ++ indices
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP TABLE IF EXISTS ${info.tables.writeAhead.name.full};") // note: actually gets dropped by jdbc store
}
