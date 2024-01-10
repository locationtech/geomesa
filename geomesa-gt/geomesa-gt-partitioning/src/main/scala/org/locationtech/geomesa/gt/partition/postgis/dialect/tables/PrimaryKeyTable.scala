/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

import org.geotools.jdbc.MetadataTablePrimaryKeyFinder

import java.util.Locale

/**
 * Primary key table used by the JDBC data store to specify primary key columns
 */
object PrimaryKeyTable extends PrimaryKeyTable with AdvisoryLock {
  override protected val lockId: Long = 6133394343366639763L
}

class PrimaryKeyTable extends Sql {

  val Name: TableName = TableName(MetadataTablePrimaryKeyFinder.DEFAULT_TABLE.toLowerCase(Locale.US))

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    // we need to define the primary key separately since the main view can't have any primary key columns
    val table = s"${info.schema.quoted}.${Name.quoted}"
    val create =
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  table_schema character varying,
         |  table_name character varying,
         |  pk_column_idx integer,
         |  pk_column character varying,
         |  pk_policy character varying,
         |  pk_sequence character varying
         |);""".stripMargin
    val cleanup = s"DELETE FROM $table WHERE table_schema = ? AND table_name = ?;"
    val entry = s"INSERT INTO $table(table_schema, table_name, pk_column_idx, pk_column) VALUES (?, ?, ?, ?);"
    ex.execute(create)
    ex.executeUpdate(cleanup, Seq(info.schema.raw, info.tables.view.name.raw))
    ex.executeUpdate(entry, Seq(info.schema.raw, info.tables.view.name.raw, 0, "fid"))
  }

  override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val rs = ex.cx.getMetaData.getTables(null, info.schema.raw, Name.raw, null)
    val exists = try { rs.next() } finally { rs.close() }
    if (exists) {
      val entry = s"DELETE FROM ${info.schema.quoted}.${Name.quoted} WHERE table_schema = ? AND table_name = ?;"
      ex.executeUpdate(entry, Seq(info.schema.raw, info.tables.view.name.raw))
    }
  }
}
