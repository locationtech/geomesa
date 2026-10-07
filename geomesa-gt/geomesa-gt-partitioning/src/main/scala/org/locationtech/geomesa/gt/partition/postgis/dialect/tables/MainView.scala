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
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData

/**
 * Main view of all the partitions and write ahead table. This should accept and reads and writes.
 */
object MainView extends SqlStatements {

  import SessionDataSource.AuthConfigName

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    val visibilityFilter = info.cols.vis.map { vis =>
      val cap = SftUserData.VisibilityDecisionArrayMaxValues.get(info.userData)
      s"""(
         |  (SELECT count <= $cap FROM sidecar_count) AND (
         |    ${vis.quoted} = ANY(ARRAY(SELECT unnest(values) FROM allowed_values)) OR
         |    (${vis.quoted} IS NULL AND (SELECT pg_vis(NULL::varchar, values) FROM auths))
         |  )
         |) OR (
         |  NOT (SELECT count <= $cap FROM sidecar_count) AND
         |  pg_vis(${vis.quoted}, (SELECT values FROM auths))
         |)""".stripMargin.replace('\n', ' ')
    }
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
    val numericFilter = filter(
      Seq(s"NOT EXISTS (SELECT FROM ${info.tables.writeAheadRetirements.name.qualified} retired " +
          "WHERE retired.partition_oid = write_ahead.tableoid)") ++ visibilityFilter
    )
    def ageOffFilter(alias: String): String = filter(
      Seq(s"NOT EXISTS (SELECT FROM ${info.tables.ageOffQueue.name.qualified} queue " +
          s"WHERE queue.partition_oid = $alias.tableoid)") ++ visibilityFilter
    )
    val ctes = info.cols.vis.fold("") { _ =>
      val table = info.tables.visibilityValues.name.qualified
      s"""WITH auths AS MATERIALIZED (
         |  SELECT string_to_array(current_setting('$AuthConfigName', true), ',') AS values
         |), sidecar_count AS MATERIALIZED (
         |  SELECT count(*) AS count FROM $table
         |), allowed_values AS MATERIALIZED (
         |  SELECT array_agg(vis) AS values
         |  FROM $table CROSS JOIN auths
         |  WHERE (SELECT count <= ${SftUserData.VisibilityDecisionArrayMaxValues.get(info.userData)} FROM sidecar_count)
         |    AND pg_vis(vis, auths.values)
         |)
         |""".stripMargin
    }
    val select =
      s"""SELECT * FROM ${info.tables.writeAhead.name.qualified} write_ahead$numericFilter UNION ALL
         |SELECT * FROM ${info.tables.writeAheadPartitions.name.qualified} write_ahead_partition$writeAheadPartitionsFilter UNION ALL
         |SELECT * FROM ${info.tables.mainPartitions.name.qualified} main_partition${ageOffFilter("main_partition")} UNION ALL
         |SELECT * FROM ${info.tables.spillPartitions.name.qualified} spill_partition${ageOffFilter("spill_partition")};""".stripMargin
    val body = if (ctes.isEmpty) { select } else { s"$ctes$select" }
    Seq(s"CREATE OR REPLACE VIEW ${info.tables.view.name.qualified} AS\n$body")
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP VIEW IF EXISTS ${info.tables.view.name.qualified};")
}
