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

  val TableName = "geomesa_userdata"

  import PartitionedPostgisDialect.Config

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    val create =
      s"""CREATE TABLE IF NOT EXISTS "${info.schema}".$TableName (
         |  type_name text not null,
         |  key text not null,
         |  value text not null
         |);""".stripMargin
    ex.execute(create)

    val insertSql =
      s"""INSERT INTO "${info.schema}".$TableName (type_name, key, value) VALUES (?, ?, ?);"""

    def insert(config: String, value: Option[String]): Unit =
      value.foreach(v => ex.executeUpdate(insertSql, Seq(info.name, config, v)))

    insert(SimpleFeatureTypes.Configs.DefaultDtgField, Some(info.cols.dtg.raw))
    insert(Config.IntervalHours, Some(Integer.toString(info.partitions.hoursPerPartition)))
    insert(Config.MaxPartitions, info.partitions.maxPartitions.map(Integer.toString))
    insert(Config.WriteAheadTableSpace, info.tables.writeAhead.tablespace)
    insert(Config.WriteAheadPartitionsTableSpace, info.tables.writeAheadPartitions.tablespace)
    insert(Config.MainTableSpace, info.tables.mainPartitions.tablespace)
    insert(Config.CronMinute, info.partitions.cronMinute.map(Integer.toString))
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
