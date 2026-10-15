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
 * Stores the distinct non-null visibility expressions for a feature type.
 */
object VisibilityValuesTable extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    info.cols.vis.toSeq.flatMap { vis =>
      val table = info.tables.visibilityValues.name.qualified
      val column = escape("vis")
      val create = s"CREATE TABLE IF NOT EXISTS $table ($column character varying PRIMARY KEY);"
      val sources = Seq(
        info.tables.writeAhead.name,
        info.tables.writeAheadPartitions.name,
        info.tables.mainPartitions.name,
        info.tables.spillPartitions.name)
      val backfill = sources.map { source =>
        s"""INSERT INTO $table ($column)
           |SELECT DISTINCT ${vis.quoted} FROM ${source.qualified}
           |WHERE ${vis.quoted} IS NOT NULL
           |ON CONFLICT ($column) DO NOTHING;""".stripMargin
      }
      create +: backfill
    }
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP TABLE IF EXISTS ${info.tables.visibilityValues.name.qualified};")
}
