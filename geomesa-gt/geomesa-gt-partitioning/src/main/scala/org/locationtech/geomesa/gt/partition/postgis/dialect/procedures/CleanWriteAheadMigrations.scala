/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

/**
 * Removes migration records and cron jobs for write-ahead partitions that have been detached
 */
object CleanWriteAheadMigrations extends SqlProcedure with CronSchedule {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeIdentifier}_clean_wa_migrations")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeIdentifier}-clean-wa-migrations")

  override protected def createStatements(info: TypeInfo): Seq[String] =
    Seq(proc(info)) ++ super.createStatements(info)

  override protected def schedule(info: TypeInfo): SqlLiteral = SqlLiteral("* * * * *")

  override protected def invocation(info: TypeInfo): SqlLiteral = SqlLiteral(s"CALL ${name(info).quoted}()")

  private def proc(info: TypeInfo): String = {
    s"""CREATE OR REPLACE PROCEDURE ${info.schema.quoted}.${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      migrated_partition text;
       |      detach_job_name text;
       |    BEGIN
       |      LOOP
       |        SELECT migration.source_partition INTO migrated_partition
       |          FROM ${info.tables.writeAheadMigrations.name.qualified} migration
       |          WHERE NOT EXISTS (
       |            SELECT FROM pg_catalog.pg_inherits
       |            WHERE inhparent = ${info.tables.writeAheadPartitions.name.asRegclass}
       |              AND inhrelid = to_regclass(migration.source_partition)
       |          )
       |          LIMIT 1;
       |        EXIT WHEN migrated_partition IS NULL;
       |
       |        detach_job_name := '${info.typeIdentifier}-detach-' || split_part(migrated_partition, '.', 2);
       |        IF EXISTS (SELECT FROM cron.job WHERE jobname = detach_job_name) THEN
       |          PERFORM cron.unschedule(detach_job_name);
       |        END IF;
       |        DELETE FROM ${info.tables.writeAheadMigrations.name.qualified}
       |          WHERE source_partition = migrated_partition;
       |        COMMIT;
       |      END LOOP;
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
