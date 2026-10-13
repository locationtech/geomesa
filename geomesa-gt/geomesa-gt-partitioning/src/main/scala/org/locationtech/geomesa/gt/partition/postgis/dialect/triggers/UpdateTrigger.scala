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
 * Trigger to delegate updates from the main view to the sub tables
 */
object UpdateTrigger extends SqlTriggerFunction {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"update_to_${info.typeIdentifier}")

  override protected def table(info: TypeInfo): TableIdentifier = info.tables.view.name

  override protected def action: String = "INSTEAD OF UPDATE"

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(function(info)) ++ super.createStatements(info)

  private def function(info: TypeInfo): String = {
    val updateFields = info.cols.all.map(c => s"${c.quoted} = NEW.${c.quoted}").mkString(",")
    val where = s"${info.cols.fid.quoted} = OLD.${info.cols.fid.quoted} AND ${info.cols.dtg.quoted} = OLD.${info.cols.dtg.quoted}"
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

    s"""CREATE OR REPLACE FUNCTION ${info.schema.quoted}.${name(info).quoted}() RETURNS trigger AS
       |  $$BODY$$
       |    DECLARE
       |      del_count integer;
       |    BEGIN
       |      UPDATE ${info.tables.writeAhead.name.qualified} target SET $updateFields WHERE $where AND $notRetired;
       |      IF NOT FOUND THEN
       |        DELETE FROM ${info.tables.writeAheadPartitions.name.qualified} WHERE $where AND $notMigrated;
       |        GET DIAGNOSTICS del_count := ROW_COUNT;
       |        IF del_count > 0 THEN
       |          INSERT INTO ${info.tables.writeAhead.name.qualified} VALUES(NEW.*);
       |        ELSE
       |          DELETE FROM ${info.tables.mainPartitions.name.qualified} target WHERE $where AND $notAgedOff;
       |          GET DIAGNOSTICS del_count := ROW_COUNT;
       |          IF del_count > 0 THEN
       |            INSERT INTO ${info.tables.writeAhead.name.qualified} VALUES(NEW.*);
       |          ELSE
       |            DELETE FROM ${info.tables.spillPartitions.name.qualified} target WHERE $where AND $notAgedOff;
       |            GET DIAGNOSTICS del_count := ROW_COUNT;
       |            IF del_count > 0 THEN
       |              INSERT INTO ${info.tables.writeAhead.name.qualified} VALUES(NEW.*);
       |            ELSE
       |              RETURN NULL;
       |            END IF;
       |          END IF;
       |        END IF;
       |      END IF;
       |      RETURN NEW;
       |    END;
       |  $$BODY$$
       |LANGUAGE plpgsql VOLATILE
       |COST 100;""".stripMargin
  }
}
