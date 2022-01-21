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
 * Stores tablespaces used by each feature type
 */
object PartitionTablespacesTable extends Sql {

  val TableName = "partition_tablespaces"

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val create =
      s"""CREATE TABLE IF NOT EXISTS "${info.schema}".$TableName (
         |  type_name text not null,
         |  table_type text not null,
         |  table_space text
         |);""".stripMargin
    ex.execute(create)

    val insertSql =
      s"""INSERT INTO "${info.schema}".$TableName (type_name, table_type, table_space) VALUES (?, ?, ?);"""

    def insert(suffix: String, table: TableConfig): Unit =
      ex.executeUpdate(insertSql, Seq(info.name, suffix, table.tablespace.orNull))

    insert(WriteAheadTableSuffix, info.tables.writeAhead)
    insert(PartitionedWriteAheadTableSuffix, info.tables.writeAheadPartitions)
    insert(PartitionedTableSuffix, info.tables.mainPartitions)
  }

  override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val rs = ex.cx.getMetaData.getTables(null, info.schema, TableName, null)
    val exists = try { rs.next() } finally { rs.close() }
    if (exists) {
      val entry = s"""DELETE FROM "${info.schema}"."$TableName" WHERE type_name = ?;"""
      ex.executeUpdate(entry, Seq(info.name))
    }
  }
}
