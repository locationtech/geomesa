/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

/**
 * Stores main partitions that have data inserted out-of-order, which may end up impacting scan performance
 */
object SortQueueTable extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    val create =
      s"""CREATE TABLE IF NOT EXISTS ${info.tables.sortQueue.name.qualified} (
         |  partition_name text,
         |  unsorted_count bigint,
         |  enqueued timestamp without time zone
         |);""".stripMargin
    Seq(create)
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP TABLE IF EXISTS ${info.tables.sortQueue.name.qualified};")
}
