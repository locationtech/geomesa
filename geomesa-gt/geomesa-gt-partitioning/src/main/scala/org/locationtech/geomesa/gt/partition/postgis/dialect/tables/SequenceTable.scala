/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

object SequenceTable extends SequenceTable with AdvisoryLock {
  override protected val lockId: Long = 8479421144957800283L
}

class SequenceTable extends Sql {

  val Name: TableName = TableName("geomesa_wa_seq")

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val table = TableIdentifier(info.schema.raw, Name.raw)
    val create =
      s"""CREATE TABLE IF NOT EXISTS ${table.qualified} (
         |  type_name text PRIMARY KEY,
         |  value smallint NOT NULL CHECK (value >= 0 AND value <= 999)
         |);""".stripMargin

    ex.execute(create)

    val insertSql =
      s"INSERT INTO ${table.qualified} (type_name, value) VALUES (?, ?) " +
          s"ON CONFLICT (type_name) DO NOTHING;"

    ex.executeUpdate(insertSql, Seq(info.typeName, 0))
  }

  override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val rs = ex.cx.getMetaData.getTables(null, info.schema.raw, Name.raw, null)
    val exists = try { rs.next() } finally { rs.close() }
    if (exists) {
      ex.executeUpdate(s"DELETE FROM ${info.schema.quoted}.${Name.quoted} WHERE type_name = ?;", Seq(info.typeName))
    }
  }
}
