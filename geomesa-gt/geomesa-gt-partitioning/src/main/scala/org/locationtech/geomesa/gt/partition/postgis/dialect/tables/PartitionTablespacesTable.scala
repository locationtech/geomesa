/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
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
object PartitionTablespacesTable extends PartitionTablespacesTable with AdvisoryLock {
  override protected val lockId: Long = 2005234735580322669L
}

class PartitionTablespacesTable extends Sql {

  val Name: TableName = TableName("partition_tablespaces")

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val table = s"${info.schema.quoted}.${Name.quoted}"
    val create =
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  type_name text not null,
         |  table_type text not null,
         |  table_space text
         |);""".stripMargin
    ex.execute(create)

    val insertSql =
      s"INSERT INTO $table (type_name, table_type, table_space) VALUES (?, ?, ?) ON CONFLICT DO NOTHING;"

    def insert(suffix: String, table: TableConfig): Unit =
      ex.executeUpdate(insertSql, Seq(info.typeName, suffix, table.tablespace.map(_.raw).orNull))

    insert(WriteAheadTableSuffix.raw, info.tables.writeAhead)
    insert(PartitionedWriteAheadTableSuffix.raw, info.tables.writeAheadPartitions)
    insert(PartitionedTableSuffix.raw, info.tables.mainPartitions)
  }

  override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val rs = ex.cx.getMetaData.getTables(null, info.schema.raw, Name.raw, null)
    val exists = try { rs.next() } finally { rs.close() }
    if (exists) {
      ex.executeUpdate(s"DELETE FROM ${info.schema.quoted}.${Name.quoted} WHERE type_name = ?;", Seq(info.typeName))
    }
  }
}
