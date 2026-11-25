/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
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

  private val TempView = "geomesa_view_definition"

  /**
   * Where visible clause. Assumes user auths are accessible via `user_auths.vis`
   *
   * @param table table or alias
   * @param col vis column
   * @return
   */
  def whereVisible(table: String, col: ColumnName): String =
    s"$table.${col.quoted} IS NOT NULL AND $table.${col.quoted} <> '' AND $table.${col.quoted}::tsquery @@ user_auths.vis"

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    // if visibilities are enabled, filter each branch by evaluating the hidden '_vis' column against the
    // caller's authorizations, stamped into the 'geomesa.auths' session variable by SessionDataSource

    val withUserAuths = info.cols.vis.fold("") { _ =>
      s"""  -- compute the user's authorizations as a native tsvector
         |  -- use NOT MATERIALIZED so that it can be pushed into each table scan
         |  WITH user_auths AS NOT MATERIALIZED (
         |    SELECT array_to_tsvector(string_to_array(current_setting('$AuthConfigName', true), ',')) AS vis
         |  )""".stripMargin
    }
    val joinUserAuths = info.cols.vis.fold("")(_ => "CROSS JOIN user_auths")
    def andWhereVisible(alias: String): String = info.cols.vis.fold("")(v => s"AND ${whereVisible(alias, v)}")

    // include the data from the previous generation of this type, if there is one. columns are matched by name,
    // with nulls for any columns that don't exist in the previous generation
    // note: the previous view already filters on visibilities and chains to any generations before it
    val previous = info.previous.fold("") { prev =>
      val existing = prev.cols.all.map(_.raw).toSet
      val cols = Seq(s"prev.${prev.cols.fid.quoted}") ++ info.cols.all.map { col =>
        // note: the null is typed based on the write ahead table, so that the view column keeps its type modifiers
        if (existing.contains(col.raw)) { s"prev.${col.quoted}" } else { s"(NULL::${info.tables.writeAhead.name.qualified}).${col.quoted}" }
      }
      s"""
         |  UNION ALL
         |  SELECT ${cols.mkString(", ")} FROM ${prev.tables.view.name.qualified} prev""".stripMargin
    }

    val select =
      s"""$withUserAuths
         |  SELECT wa.* FROM ${info.tables.writeAhead.name.qualified} wa $joinUserAuths WHERE
         |    NOT EXISTS (
         |      SELECT FROM ${info.tables.writeAheadRetirements.name.qualified} retired
         |        WHERE retired.partition_oid = wa.tableoid
         |    ) ${andWhereVisible("wa")}
         |  UNION ALL
         |  SELECT wap.* FROM ${info.tables.writeAheadPartitions.name.qualified} wap $joinUserAuths WHERE
         |    NOT EXISTS (
         |      SELECT FROM ${info.tables.writeAheadMigrations.name.qualified} migration
         |        WHERE wap.${info.cols.dtg.quoted} >= migration.partition_start
         |        AND wap.${info.cols.dtg.quoted} < migration.partition_end
         |    ) ${andWhereVisible("wap")}
         |  UNION ALL
         |  SELECT mp.* FROM ${info.tables.mainPartitions.name.qualified} mp $joinUserAuths WHERE
         |    NOT EXISTS (
         |      SELECT FROM ${info.tables.ageOffQueue.name.qualified} queue
         |        WHERE queue.partition_oid = mp.tableoid
         |    ) ${andWhereVisible("mp")}
         |  UNION ALL
         |  SELECT sp.* FROM ${info.tables.spillPartitions.name.qualified} sp $joinUserAuths WHERE
         |    NOT EXISTS (
         |      SELECT FROM ${info.tables.ageOffQueue.name.qualified} queue
         |        WHERE queue.partition_oid = sp.tableoid
         |    ) ${andWhereVisible("sp")}$previous""".stripMargin

    // replacing a view requires an exclusive lock on it, which will block behind any queries against the view (and
    // will then block any new queries). to avoid that, we only replace the view if the definition has changed, which
    // we determine by comparing it to a temporary view
    // note: if we don't have the privileges to create temporary views, we fall back to always replacing the view
    val create =
      s"""DO $$VIEW$$
         |DECLARE
         |  existing_def text;
         |  updated_def text;
         |BEGIN
         |  IF to_regclass(${literal(info.tables.view.name.qualified)}) IS NOT NULL THEN
         |    BEGIN
         |      CREATE TEMP VIEW $TempView AS
         |$select;
         |      existing_def := pg_get_viewdef(${info.tables.view.name.asRegclass});
         |      updated_def := pg_get_viewdef('pg_temp.$TempView'::regclass);
         |      DROP VIEW pg_temp.$TempView;
         |    EXCEPTION WHEN insufficient_privilege THEN
         |      existing_def := NULL;
         |    END;
         |  END IF;
         |  IF existing_def IS NULL OR existing_def <> updated_def THEN
         |    CREATE OR REPLACE VIEW ${info.tables.view.name.qualified} AS
         |$select;
         |  END IF;
         |END$$VIEW$$;""".stripMargin

    Seq(create)
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] =
    Seq(s"DROP VIEW IF EXISTS ${info.tables.view.name.qualified};")
}
