/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

import org.locationtech.geomesa.gt.partition.postgis.dialect.auths.SessionDataSource

/**
 * Main view of all the partitions and write ahead table. This should accept and reads and writes.
 */
object MainView extends SqlStatements {

  import SessionDataSource.AuthConfigName

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    // if visibilities are enabled, filter each branch by evaluating the hidden '_vis' column against the
    // caller's authorizations, stamped into the 'geomesa.auths' session variable by SessionDataSource.
    // the auths array is built in an uncorrelated sub-select so that it's evaluated once per query (as an
    // init plan) rather than re-running current_setting/string_to_array for every row
    val visibilityFilter =
      info.cols.vis.map(vis => s"pg_vis(${vis.quoted}, (SELECT string_to_array(current_setting('$AuthConfigName', true), ',')))")
    def filter(conditions: Seq[String]): String =
      if (conditions.isEmpty) { "" } else { conditions.mkString(" WHERE ", " AND ", "") }
    val writeAheadPartitionsFilter = filter(
      Seq(
        s"""NOT EXISTS (
           |  SELECT FROM ${info.tables.writeAheadMigrations.name.qualified} migration
           |  WHERE write_ahead_partition.${info.cols.dtg.quoted} >= migration.partition_start
           |    AND write_ahead_partition.${info.cols.dtg.quoted} < migration.partition_end
           |)""".stripMargin
      ) ++ visibilityFilter
    )
    val defaultFilter = filter(visibilityFilter.toSeq)
    Seq(
      s"""CREATE OR REPLACE VIEW ${info.tables.view.name.qualified} AS
         |  SELECT * FROM ${info.tables.writeAhead.name.qualified}$defaultFilter UNION ALL
         |  SELECT * FROM ${info.tables.writeAheadPartitions.name.qualified} write_ahead_partition$writeAheadPartitionsFilter UNION ALL
         |  SELECT * FROM ${info.tables.mainPartitions.name.qualified}$defaultFilter UNION ALL
         |  SELECT * FROM ${info.tables.spillPartitions.name.qualified}$defaultFilter;""".stripMargin
    )
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP VIEW IF EXISTS ${info.tables.view.name.qualified};")
}
