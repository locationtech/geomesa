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
 * Removes retired numeric write-ahead tables when readers permit it. The retirement marker hides
 * their rows from new queries; rotation can delete the originals and reuse a table before cleanup.
 */
object CleanWriteAheadRetirements extends SqlProcedure with CronSchedule {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeIdentifier}_clean_wa_retirements")

  override def jobName(info: TypeInfo): SqlLiteral = SqlLiteral(s"${info.typeIdentifier}-clean-wa-retirements")

  override protected def schedule(info: TypeInfo): SqlLiteral = SqlLiteral("* * * * *")

  override protected def invocation(info: TypeInfo): SqlLiteral =
    SqlLiteral(s"CALL ${info.schema.quoted}.${name(info).quoted}()")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info)) ++ super.createStatements(info)

  private def proc(info: TypeInfo): String =
    s"""CREATE OR REPLACE PROCEDURE ${info.schema.quoted}.${name(info).quoted}() LANGUAGE plpgsql AS
       |  $$BODY$$
       |    DECLARE
       |      retired record;
       |      is_attached boolean;
       |      is_retired boolean;
       |      table_exists boolean;
       |    BEGIN
       |      FOR retired IN
       |        SELECT * FROM ${info.tables.writeAheadRetirements.name.qualified} ORDER BY source_partition
       |      LOOP
       |        -- first check if the table is attached and try to detach it - this requires ACCESS EXCLUSIVE
       |        SELECT EXISTS (SELECT FROM pg_inherits
       |            WHERE inhparent = ${info.tables.writeAhead.name.asRegclass}
       |              AND inhrelid = to_regclass(retired.source_partition)) INTO is_attached;
       |
       |        IF is_attached THEN
       |          BEGIN
       |            -- match log rotation's lock order, and never queue behind a reader or another cleanup.
       |            -- we need EXCLUSIVE on both parent and child to NO INHERIT it, but that is the same as DROP requires.
       |            -- note that queries will still lock the child table, even though it is excluded due to _wa_retirements
       |            LOCK TABLE ONLY ${info.tables.writeAhead.name.qualified} IN ACCESS EXCLUSIVE MODE NOWAIT;
       |            EXECUTE 'LOCK TABLE ONLY ' || retired.source_partition || ' IN ACCESS EXCLUSIVE MODE NOWAIT';
       |            -- re-check existence now that we have our locks
       |            SELECT EXISTS (
       |              SELECT FROM ${info.tables.writeAheadRetirements.name.qualified}
       |                WHERE partition_oid = retired.partition_oid AND enqueued = retired.enqueued)
       |              INTO is_retired;
       |            SELECT COALESCE(to_regclass(retired.source_partition)::oid = retired.partition_oid, false)
       |              INTO table_exists;
       |            SELECT EXISTS (SELECT FROM pg_inherits
       |              WHERE inhparent = ${info.tables.writeAhead.name.asRegclass} AND inhrelid = to_regclass(retired.source_partition))
       |              INTO is_attached;
       |            IF is_retired AND table_exists AND is_attached THEN
       |              EXECUTE 'ALTER TABLE ' || retired.source_partition || ' NO INHERIT ${info.tables.writeAhead.name.qualified}';
       |              is_attached := false;
       |            END IF;
       |          EXCEPTION WHEN lock_not_available OR undefined_table THEN
       |            RAISE DEBUG 'Deferring removal of numeric write-ahead table %', retired.source_partition;
       |          END;
       |          COMMIT; -- release locks
       |        END IF;
       |
       |        -- now try to drop the table, if it's no longer attached.
       |        -- there shouldn't be any contention on the child table at this point
       |        IF NOT is_attached THEN
       |          -- serialize with rotation and cleanup procedures
       |          LOCK TABLE ONLY ${info.tables.writeAhead.name.qualified} IN SHARE UPDATE EXCLUSIVE MODE;
       |          -- verify again that table exists and isn't attached
       |          SELECT EXISTS (SELECT FROM ${info.tables.writeAheadRetirements.name.qualified}
       |            WHERE partition_oid = retired.partition_oid AND enqueued = retired.enqueued)
       |            INTO is_retired;
       |          SELECT COALESCE(to_regclass(retired.source_partition)::oid = retired.partition_oid, false)
       |            INTO table_exists;
       |          SELECT EXISTS (
       |            SELECT FROM pg_inherits
       |              WHERE inhparent = ${info.tables.writeAhead.name.asRegclass} AND inhrelid = to_regclass(retired.source_partition))
       |            INTO is_attached;
       |          IF is_retired AND NOT is_attached THEN
       |            IF table_exists THEN
       |              EXECUTE 'DROP TABLE IF EXISTS' || retired.source_partition;
       |            END IF;
       |            DELETE FROM ${info.tables.writeAheadRetirements.name.qualified}
       |              WHERE partition_oid = retired.partition_oid AND enqueued = retired.enqueued;
       |          END IF;
       |        END IF;
       |        COMMIT; -- release locks
       |      END LOOP;
       |    END;
       |  $$BODY$$;""".stripMargin
}
