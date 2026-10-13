/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package triggers

/**
 * Trigger to delegate deletes from the main view to the sub tables
 */
object DeleteTrigger extends SqlTriggerFunction {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"delete_from_${info.typeIdentifier}")

  override protected def table(info: TypeInfo): TableIdentifier = info.tables.view.name

  override protected def action: String = "INSTEAD OF DELETE"

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function(info)) ++ super.createStatements(info)

  private def function(info: TypeInfo): String = {
    // Migrated write-ahead rows are hidden by the view; mutate their visible main/spill copies instead.
    val notMigrated =
      s"""NOT EXISTS (
         |  SELECT FROM ${info.tables.writeAheadMigrations.name.qualified} migration
         |  WHERE OLD.${info.cols.dtg.quoted} >= migration.partition_start
         |    AND OLD.${info.cols.dtg.quoted} < migration.partition_end
         |)""".stripMargin
    val notRetired = s"NOT EXISTS (SELECT FROM ${info.tables.writeAheadRetirements.name.qualified} retired " +
        "WHERE retired.partition_oid = target.tableoid)"
    val notAgedOff = s"NOT EXISTS (SELECT FROM ${info.tables.ageOffQueue.name.qualified} queue " +
        "WHERE queue.partition_oid = target.tableoid)"
    def delete(table: TableConfig): String = {
      val filter = if (table == info.tables.writeAhead) {
        s" AND $notRetired"
      } else if (table == info.tables.writeAheadPartitions) {
        s" AND $notMigrated"
      } else if (table == info.tables.mainPartitions || table == info.tables.spillPartitions) {
        s" AND $notAgedOff"
      } else {
        ""
      }
      s"DELETE FROM ${table.name.qualified} target WHERE ${info.cols.fid.quoted} = OLD.${info.cols.fid.quoted} AND ${info.cols.dtg.quoted} = OLD.${info.cols.dtg.quoted}$filter"
    }

    s"""CREATE OR REPLACE FUNCTION ${info.schema.quoted}.${name(info).quoted}() RETURNS trigger AS
       |  $$BODY$$
       |    DECLARE
       |      del_count integer;
       |    BEGIN
       |      ${delete(info.tables.writeAhead)};
       |      GET DIAGNOSTICS del_count := ROW_COUNT;
       |      IF del_count = 0 THEN
       |        ${delete(info.tables.writeAheadPartitions)};
       |        GET DIAGNOSTICS del_count := ROW_COUNT;
       |        IF del_count = 0 THEN
       |          ${delete(info.tables.mainPartitions)};
       |          GET DIAGNOSTICS del_count := ROW_COUNT;
       |          IF del_count = 0 THEN
       |            ${delete(info.tables.spillPartitions)};
       |            GET DIAGNOSTICS del_count := ROW_COUNT;
       |            IF del_count = 0 THEN
       |              RETURN NULL;
       |            END IF;
       |          END IF;
       |        END IF;
       |      END IF;
       |      RETURN OLD;
       |    END;
       |  $$BODY$$
       |LANGUAGE plpgsql VOLATILE
       |COST 100;""".stripMargin
  }
}
