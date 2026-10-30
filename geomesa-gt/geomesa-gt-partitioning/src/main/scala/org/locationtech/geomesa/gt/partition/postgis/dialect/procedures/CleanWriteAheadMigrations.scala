/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

/**
 * Removes detached tables and continually schedules or recovers one write-ahead detach per parent
 */
object CleanWriteAheadMigrations extends SqlProcedure with CronSchedule {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeIdentifier}_clean_wa_migrations")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeIdentifier}-clean-wa-migrations")

  def detachJobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeIdentifier}-detach-wa")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info)) ++ super.createStatements(info)

  override protected def schedule(info: TypeInfo): SqlLiteral = SqlLiteral("* * * * *")

  override protected def invocation(info: TypeInfo): SqlLiteral =
    SqlLiteral(s"CALL ${info.schema.quoted}.${name(info).quoted}()")

  override protected def dropStatements(info: TypeInfo): Seq[String] = Seq(
    s"""DO $$$$ DECLARE migration record;
       |BEGIN
       |${unscheduleSql(detachJobName(info).quoted)}
       |  FOR migration IN SELECT source_partition FROM ${info.tables.writeAheadMigrations.name.qualified} LOOP
       |    IF EXISTS (SELECT FROM cron.job WHERE jobname = ${literal(info.typeIdentifier + "-detach-")} ||
       |        split_part(migration.source_partition, '.', 2)) THEN
       |      PERFORM cron.unschedule(${literal(info.typeIdentifier + "-detach-")} || split_part(migration.source_partition, '.', 2));
       |    END IF;
       |  END LOOP;
       |END$$$$;""".stripMargin
  ) ++ super.dropStatements(info)

  private def proc(info: TypeInfo): String =
    s"""CREATE OR REPLACE PROCEDURE ${info.schema.quoted}.${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      migrated record;
       |      partition_name text;
       |      detach_pending boolean;
       |      legacy_job_name text;
       |      detach_invocation text;
       |    BEGIN
       |      FOR migrated IN
       |        SELECT source_partition FROM ${info.tables.writeAheadMigrations.name.qualified} ORDER BY source_partition
       |      LOOP
       |        -- Replace the old per-partition jobs with one worker for the parent.
       |        legacy_job_name := ${literal(info.typeIdentifier + "-detach-")} || split_part(migrated.source_partition, '.', 2);
       |        IF EXISTS (SELECT FROM cron.job WHERE jobname = legacy_job_name) THEN
       |          PERFORM cron.unschedule(legacy_job_name);
       |        END IF;
       |        IF NOT EXISTS (SELECT FROM pg_inherits
       |            WHERE inhparent = ${info.tables.writeAheadPartitions.name.asRegclass}
       |              AND inhrelid = to_regclass(migrated.source_partition)) THEN
       |          BEGIN
       |            IF to_regclass(migrated.source_partition) IS NOT NULL THEN
       |              EXECUTE 'LOCK TABLE ONLY ' || migrated.source_partition || ' IN ACCESS EXCLUSIVE MODE NOWAIT';
       |              EXECUTE 'DROP TABLE ' || migrated.source_partition;
       |            END IF;
       |            DELETE FROM ${info.tables.writeAheadMigrations.name.qualified}
       |              WHERE source_partition = migrated.source_partition;
       |          EXCEPTION WHEN lock_not_available THEN
       |            RAISE DEBUG 'Deferring removal of detached write-ahead partition %', migrated.source_partition;
       |          END;
       |        END IF;
       |        COMMIT;
       |      END LOOP;
       |
       |      SELECT migration.source_partition, inherits.inhdetachpending INTO partition_name, detach_pending
       |        FROM ${info.tables.writeAheadMigrations.name.qualified} migration
       |        JOIN pg_inherits inherits ON inherits.inhrelid = to_regclass(migration.source_partition)
       |          AND inherits.inhparent = ${info.tables.writeAheadPartitions.name.asRegclass}
       |        ORDER BY inherits.inhdetachpending DESC, migration.enqueued, migration.source_partition
       |        LIMIT 1;
       |      IF FOUND THEN
       |        detach_invocation := ${literal(s"ALTER TABLE ${info.tables.writeAheadPartitions.name.qualified} DETACH PARTITION ")} ||
       |          partition_name || CASE WHEN detach_pending THEN ' FINALIZE' ELSE ' CONCURRENTLY' END;
       |        PERFORM cron.schedule(${detachJobName(info).quoted}, '* * * * *', detach_invocation);
       |      ELSIF EXISTS (SELECT FROM cron.job WHERE jobname = ${detachJobName(info).quoted}) THEN
       |        PERFORM cron.unschedule(${detachJobName(info).quoted});
       |      END IF;
       |    END;
       |  $$BODY$$;""".stripMargin
}
