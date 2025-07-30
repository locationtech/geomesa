/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.utils.io.WithClose

import java.sql.Connection

/**
 * Stores feature type user data
 */
object UserDataTable extends UserDataTable with AdvisoryLock {
  override protected val lockId: Long = 8778078099312765227L
}

class UserDataTable extends Sql {

  val Name: TableName = TableName("geomesa_userdata")

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val table = TableIdentifier(info.schema.raw, Name.raw)
    val cName = TableName(Name.raw + "_pkey")
    val alter =
      s"""ALTER TABLE IF EXISTS ${table.quoted} ADD COLUMN IF NOT EXISTS mutable boolean default false;"""
    val create =
      s"""CREATE TABLE IF NOT EXISTS ${table.quoted} (
         |  type_name text not null,
         |  key text not null,
         |  value text not null,
         |  mutable boolean default false
         |);""".stripMargin
    val constraint =
      s"""DO $$$$
         |BEGIN
         |  IF NOT EXISTS (SELECT FROM pg_constraint WHERE conname = ${cName.asLiteral} AND conrelid = ${table.asRegclass}) THEN
         |    ALTER TABLE ${table.quoted} ADD CONSTRAINT ${cName.quoted} PRIMARY KEY (type_name, key);
         |  END IF;
         |END$$$$;""".stripMargin

    Seq(alter, create, constraint).foreach(ex.execute)

    val insertSql =
      s"INSERT INTO ${table.quoted} (type_name, key, value, mutable) VALUES (?, ?, ?, ?) " +
          s"ON CONFLICT (type_name, key) DO UPDATE SET value = EXCLUDED.value;"

    def insert(config: SftUserData[_], value: String): Unit =
      ex.executeUpdate(insertSql, Seq(info.typeName, config.key, value, config.mutable))

    insert(SftUserData.DtgField, info.cols.dtg.raw)
    insert(SftUserData.IntervalHours, String.valueOf(info.partitions.hoursPerPartition))
    if (info.partitions.pagesPerRange != SftUserData.PagesPerRange.default) {
      insert(SftUserData.PagesPerRange, String.valueOf(info.partitions.pagesPerRange))
    }
    info.partitions.maxPartitions.foreach(v => insert(SftUserData.MaxPartitions, String.valueOf(v)))
    info.partitions.cronMinute.foreach(v => insert(SftUserData.CronMinute, String.valueOf(v)))
    info.tables.writeAhead.tablespace.foreach(ts => insert(SftUserData.WriteAheadTableSpace, ts.raw))
    info.tables.writeAheadPartitions.tablespace.foreach(ts => insert(SftUserData.WriteAheadPartitionsTableSpace, ts.raw))
    info.tables.mainPartitions.tablespace.foreach(ts => insert(SftUserData.MainTableSpace, ts.raw))
    // note: all tables key off the same user data field for logged/unlogged
    if (!info.tables.mainPartitions.logged) {
      insert(SftUserData.WalLogEnabled, "false")
    }
    Seq(SftUserData.FilterWholeWorld, SftUserData.QueryInterceptors).foreach { config =>
      info.userData.get(config.key).foreach(v => insert(config, v))
    }
  }

  override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val rs = ex.cx.getMetaData.getTables(null, info.schema.raw, Name.raw, null)
    val exists = try { rs.next() } finally { rs.close() }
    if (exists) {
      ex.executeUpdate(s"DELETE FROM ${info.schema.quoted}.${Name.quoted} WHERE type_name = ?;", Seq(info.typeName))
    }
  }

  /**
   * Read user data for the given type
   *
   * @param cx connection
   * @param schema database schema
   * @param typeName feature type name
   * @return
   */
  def read(cx: Connection, schema: String, typeName: String): Map[String, String] = {
    val map = Map.newBuilder[String, String]
    val userDataSql = s"select key, value from ${escape(schema)}.${UserDataTable.Name.quoted} where type_name = ?"
    WithClose(cx.prepareStatement(userDataSql)) { statement =>
      statement.setString(1, typeName)
      WithClose(statement.executeQuery()) { rs =>
        while (rs.next()) {
          map += rs.getString(1) -> rs.getString(2)
        }
      }
    }
    map.result()
  }
}
