/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

/**
 * Stores main partitions that have data inserted out-of-order, which may end up impacting scan performance
 */
object SortQueueTable extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    val logging = if (info.tables.sortQueue.logged) { "" } else { "UNLOGGED" }
    val create =
      s"""CREATE $logging TABLE IF NOT EXISTS ${info.tables.sortQueue.name.qualified} (
         |  partition_name text,
         |  unsorted_count bigint,
         |  enqueued timestamp without time zone
         |);""".stripMargin
    Seq(create)
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP TABLE IF EXISTS ${info.tables.sortQueue.name.qualified};")
}
