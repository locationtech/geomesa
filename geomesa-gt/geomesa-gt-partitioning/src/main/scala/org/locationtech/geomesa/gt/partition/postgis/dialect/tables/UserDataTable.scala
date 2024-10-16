/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

/**
 * Stores feature type user data
 */
object UserDataTable extends UserDataTable with AdvisoryLock {
  override protected val lockId: Long = 8778078099312765227L
}

class UserDataTable extends Sql {

  val Name: TableName = TableName("geomesa_userdata")

  import PartitionedPostgisDialect.Config

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val table = TableIdentifier(info.schema.raw, Name.raw)
    val cName = TableName(Name.raw + "_pkey")
    val create =
      s"""CREATE TABLE IF NOT EXISTS ${table.quoted} (
         |  type_name text not null,
         |  key text not null,
         |  value text not null
         |);""".stripMargin
    val constraint =
      s"""DO $$$$
         |BEGIN
         |  IF NOT EXISTS (SELECT FROM pg_constraint WHERE conname = ${cName.asLiteral} AND conrelid = ${table.asRegclass}) THEN
         |    ALTER TABLE ${table.quoted} ADD CONSTRAINT ${cName.quoted} PRIMARY KEY (type_name, key);
         |  END IF;
         |END$$$$;""".stripMargin

    Seq(create, constraint).foreach(ex.execute)

    val insertSql =
      s"INSERT INTO ${table.quoted} (type_name, key, value) VALUES (?, ?, ?) " +
          s"ON CONFLICT (type_name, key) DO UPDATE SET value = EXCLUDED.value;"

    def insert(config: String, value: String): Unit =
      ex.executeUpdate(insertSql, Seq(info.typeName, config, value))

    insert(SimpleFeatureTypes.Configs.DefaultDtgField, info.cols.dtg.raw)
    insert(Config.IntervalHours, Integer.toString(info.partitions.hoursPerPartition))
    insert(Config.PagesPerRange, Integer.toString(info.partitions.pagesPerRange))
    info.partitions.maxPartitions.map(Integer.toString).foreach(insert(Config.MaxPartitions, _))
    info.partitions.cronMinute.map(Integer.toString).foreach(insert(Config.CronMinute, _))
    Seq(Config.FilterWholeWorld, SimpleFeatureTypes.Configs.QueryInterceptors).foreach { key =>
      info.userData.get(key).foreach(insert(key, _))
    }
  }

  override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val rs = ex.cx.getMetaData.getTables(null, info.schema.raw, Name.raw, null)
    val exists = try { rs.next() } finally { rs.close() }
    if (exists) {
      ex.executeUpdate(s"DELETE FROM ${info.schema.quoted}.${Name.quoted} WHERE type_name = ?;", Seq(info.typeName))
    }
  }
}
