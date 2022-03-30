/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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
object UserDataTable extends Sql {

  val Name: TableName = TableName("geomesa_userdata")

  import PartitionedPostgisDialect.Config

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val table = s"${info.schema.quoted}.${Name.quoted}"
    val create =
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  type_name text not null,
         |  key text not null,
         |  value text not null
         |);""".stripMargin
    ex.execute(create)

    val insertSql = s"INSERT INTO $table (type_name, key, value) VALUES (?, ?, ?);"

    def insert(config: String, value: Option[String]): Unit =
      value.foreach(v => ex.executeUpdate(insertSql, Seq(info.typeName, config, v)))

    insert(SimpleFeatureTypes.Configs.DefaultDtgField, Some(info.cols.dtg.raw))
    insert(Config.IntervalHours, Some(Integer.toString(info.partitions.hoursPerPartition)))
    insert(Config.MaxPartitions, info.partitions.maxPartitions.map(Integer.toString))
    insert(Config.WriteAheadTableSpace, info.tables.writeAhead.tablespace.map(_.raw))
    insert(Config.WriteAheadPartitionsTableSpace, info.tables.writeAheadPartitions.tablespace.map(_.raw))
    insert(Config.MainTableSpace, info.tables.mainPartitions.tablespace.map(_.raw))
    insert(Config.CronMinute, info.partitions.cronMinute.map(Integer.toString))
  }

  override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val rs = ex.cx.getMetaData.getTables(null, info.schema.raw, Name.raw, null)
    val exists = try { rs.next() } finally { rs.close() }
    if (exists) {
      ex.executeUpdate(s"DELETE FROM ${info.schema.quoted}.${Name.quoted} WHERE type_name = ?;", Seq(info.typeName))
    }
  }
}
